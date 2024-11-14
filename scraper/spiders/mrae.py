import datetime
import re
import sys

import scrapy
from scrapy.exceptions import CloseSpider

from ..items import DocumentItem

DOCUMENT_CATEGORIES = [
    "Avis rendus sur projets",
    "Avis rendus sur plans et programmes",
    "Examen au cas par cas et autres décisions",
    "Avis conformes",
]


class MRAESpider(scrapy.Spider):

    name = "MRAE_spider"

    # allowed_domains = ["www.mrae.developpement-durable.gouv.fr"]

    start_urls = [
        "https://www.mrae.developpement-durable.gouv.fr/les-mrae-r37.html?lang=fr"
    ]

    upload_limit_attained = False

    def check_upload_limit(self):
        """Closes the spider if the upload limit is attained."""
        if self.upload_limit_attained:
            raise CloseSpider("Closed due to max documents limit.")

    def parse(self, response):
        """Find links to regional authorities."""

        self.check_upload_limit()

        sections = response.css(
            "#contenu .liste-rubriques .rubrique_avec_sous-rubriques"
        )

        for sec in sections:
            region = (
                sec.css(".item-rubriques__body .fr-tile__title::text")
                .get()
                .strip(" \t\n")
                .replace(" - ", "-")
            )

            categories = sec.css(".item-liste-sous-rubriques")

            for cat in categories:
                category_name = cat.css(".lien-sous-rubrique::text").get()
                category_link = cat.css(".lien-sous-rubrique").attrib["href"]

                if (
                    category_name
                    == "Décisions après examens au cas par cas et autres décisions"
                ):
                    category_name = "Examen au cas par cas et autres décisions"

                if category_name in DOCUMENT_CATEGORIES:

                    yield response.follow(
                        category_link,
                        callback=self.parse_category_page,
                        cb_kwargs=dict(region=region, category_local=category_name),
                    )

    def parse_category_page(self, response, region, category_local):
        """Find links to pages containing the files."""

        def year_check(string, target_year):
            """Check if the page needs to be crawled. Input string is the link's text"""

            years_matches = re.findall(r"20\d\d", string)

            if not years_matches:
                # No year in text, might contain documents we targeted
                return True

            elif len(years_matches) == 1:
                if int(years_matches[0]) == target_year:
                    return True
                else:
                    return False

            elif len(years_matches) == 2:
                if target_year in [int(x) for x in years_matches]:
                    return True
                else:
                    range_match = re.search(
                        r"(20\d\d) à (20\d\d)|(20\d\d)-(20\d\d)", string
                    )
                    if range_match:
                        years_range_list = [
                            int(x) for x in range_match.groups() if x is not None
                        ]
                        years_range = range(
                            years_range_list[0], years_range_list[1] + 1
                        )

                        if target_year in years_range:
                            return True
                        else:  # not in range
                            return False
                    else:  # no range match
                        return False

            else:  # more than 2 years in page name
                if target_year in [int(x) for x in years_matches]:
                    return True
                else:
                    return False

        self.logger.info(f"Scraping {region} / {category_local} {self.target_year}")

        cards_list = response.css(".liste-articles .fr-card__body")

        for card in cards_list:
            link_text = card.css(".fr-card__title a::text").get()
            link_href = card.css(".fr-card__title a").attrib["href"]

            if year_check(link_text, target_year=self.target_year):
                yield response.follow(
                    link_href,
                    callback=self.parse_documents_page,
                    cb_kwargs=dict(
                        region=region, category_local=category_local, page=link_text
                    ),
                )

        # Follow next page
        next_page_link = response.css("nav.pagination a.fr-pagination__link--next")

        if next_page_link.xpath("@href"):
            yield response.follow(
                next_page_link.attrib["href"],
                callback=self.parse_category_page,
                cb_kwargs=dict(region=region, category_local=category_local),
            )

    def parse_documents_page(self, response, region, category_local, page):

        def get_project_name(projectbox):
            """Extracts the project name from the scrapy selector for the html box"""

            # Try to get the project name from bold text
            strong_matches = projectbox.css("strong::text").getall()

            strong_matches = [
                x
                for x in strong_matches
                if x.strip()
                not in [
                    "est soumis",
                    "soumis",
                    "sont soumis",
                    'Avis conforme délibéré après examen au cas par cas "ad hoc"',
                    '(Avis conforme délibéré après examen au cas par cas "ad hoc"',
                    "dispense",
                ]
            ]
            project_name = None

            if (
                len(strong_matches) == 1
                and not strong_matches[0].strip() == "est soumis"
            ):
                project_name = strong_matches[0].strip()

            else:
                box_text = "".join(projectbox.css(" *::text").getall())
                lines = box_text.split("\n")

                lines = [x.strip() for x in lines if x.strip()]

                if (
                    lines[0].strip()
                    == 'Avis conforme délibéré après examen au cas par cas "ad hoc"'
                ):
                    lines = lines[1:]

                project_name = lines[0].strip()  # get the first line only

                for line in lines[1:]:
                    # For other lines, check that they start with a lowercase letter
                    # or finish with the department number
                    if line[0].islower() or (
                        re.search(r" \((\d\d\d?|2A|2B)\)$", line)
                        and not (
                            line.startswith("Avis")
                            or line.startswith("Dossier étudié à la demande")
                        )
                    ):
                        project_name += " " + line
                    else:
                        break

            if project_name:
                project_name = project_name.replace("\n", "")
                return project_name
            else:
                return "ERROR"

        def get_year_from_first_docname(projectbox):
            first_doc_name = projectbox.css(".fr-download a::text").get().strip()
            date_match = re.search(r"20\d\d", first_doc_name)
            year = date_match.group()

            if year:
                return int(year)
            else:
                self.logger.error("ERROR: year not inferred from docname")

        def get_full_info(projectbox):
            """Get the full info from the projectbox, in a clean format.
            Can be used to later extract petitioner & decision date properly."""

            full_info = "".join(
                [x.strip("\t\r") for x in projectbox.css(" *::text").getall()[1:]]
            ).strip()

            return full_info

        # Main fuction

        self.check_upload_limit()

        filecards = response.css(".fr-download--card")

        if filecards:
            for fc in filecards:

                doc_name = fc.css(".fr-download__link::text").get().strip()
                doc_link = fc.css(".fr-download__link").attrib["href"]
                parent = fc.xpath("./..")

                if parent.xpath("name()").get() == "strong":
                    parent = parent.xpath("./..")

                if parent.css(".texte-article"):
                    # Missing projectbox
                    try:
                        preceding_p = fc.xpath("./preceding-sibling::p")[-1]
                        project = get_project_name(preceding_p)
                        full_info = get_full_info(preceding_p)
                    except IndexError:
                        self.logger.warning(
                            f"Could not find projectbox for doc {doc_name} on {response.request.url}"
                        )
                        yield

                else:
                    projectbox = parent
                    project = get_project_name(projectbox)
                    full_info = get_full_info(projectbox)

                    if fc.xpath(
                        "./preceding-sibling::p/strong[not(re:test(text(),' *(est )?soumis'))]"
                    ):
                        # Malformed projectbox
                        # e.g. https://www.mrae.developpement-durable.gouv.fr/avis-rendus-sur-projets-de-la-mrae-corse-en-2023-a1202.html
                        # Get last p containing a strong tag preceding the filebox

                        preceding_p_tags = fc.xpath(
                            "./preceding-sibling::p[strong[not(re:test(text(),' *(est )?soumis'))]]"
                        )

                        preceding_p = preceding_p_tags[-1]
                        # Get project name from this p
                        project = get_project_name(preceding_p)
                        full_info = get_full_info(preceding_p)

                doc_item = DocumentItem(
                    title=doc_name,
                    project=project,
                    authority=f"MRAe {region}",
                    category_local=category_local,
                    source_file_url=response.urljoin(doc_link),
                    source_page_url=response.request.url,
                    full_info=full_info,
                    source="www.mrae.developpement-durable.gouv.fr",
                    source_scraper="MRAe Scraper",
                )

                # Check if the doc matches the target year
                if len(re.findall(r"20\d\d", page)) != 1:  # multi-year page
                    previous_h2 = (
                        parent.xpath("./preceding-sibling::h2")[-1].css("::text").get()
                    )

                    if str(self.target_year) in previous_h2:
                        target_year_match = True
                    else:
                        target_year_match = False
                        self.logger.debug(
                            f"Discarded item on multi-year page: {doc_item['title']} | {doc_item['source_page_url']}"
                        )
                else:
                    target_year_match = True

                if (
                    target_year_match
                    and not doc_item["source_file_url"] in self.event_data
                ):

                    doc_item["year"] = self.target_year
                    yield response.follow(
                        doc_link,
                        method="HEAD",
                        callback=self.parse_document_headers,
                        cb_kwargs=dict(doc_item=doc_item, page=page),
                    )

    def parse_document_headers(self, response, doc_item, page):
        """Gets the headers of a document to extract its publication date (Last-Modified header)."""

        self.check_upload_limit()

        # Use Last-Modified header as date for the document
        # Note: this is UTC
        doc_item["headers"] = dict(response.headers.to_unicode_dict())
        last_modified = response.headers.get("Last-Modified").decode("utf-8")

        doc_item["publication_lastmodified"] = last_modified

        dt = datetime.datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z")

        yield doc_item
