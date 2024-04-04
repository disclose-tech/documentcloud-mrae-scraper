# Item Pipelines

import datetime
import re
import os
from urllib.parse import urlparse

import dateparser

# from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

from .corrections import corrections


class ParseDatePipeline:
    """Parse dates from scraped data."""

    def process_item(self, item, spider):
        """Parses date from the extracted string"""

        # Publication date

        publication_dt = datetime.datetime.strptime(
            item["publication_lastmodified"], "%a, %d %b %Y %H:%M:%S %Z"
        )

        item["publication_timestamp"] = publication_dt.isoformat() + "Z"

        item["publication_date"] = publication_dt.strftime("%Y-%m-%d")
        item["publication_time"] = publication_dt.strftime("%H:%M:%S UTC")
        item["publication_datetime"] = (
            item["publication_date"] + " " + item["publication_time"]
        )

        # Decision date

        if not item["decision_date_string"] == "ERROR":
            decision_dt = dateparser.parse(
                item["decision_date_string"], languages=["fr"]
            )
            if decision_dt:
                item["decision_date"] = decision_dt.strftime("%Y-%m-%d")

            else:
                item["decision_date"] = "ERROR"

        else:
            item["decision_date"] = "ERROR"

        return item


class CategoryPipeline:
    """Attributes the final category of the document."""

    def process_item(self, item, spider):
        if item["category_local"] == "Avis conformes":
            item["category"] = "Cas par cas"

        elif item["category_local"] == "Examen au cas par cas et autres décisions":
            item["category"] = "Cas par cas"

        elif item["category_local"] in [
            "Avis rendus sur plans et programmes",
            "Avis rendus sur projets",
        ]:
            item["category"] = "Avis"

        return item


class SourceFilenamePipeline:
    """Adds the source_filename field based on source_file_url."""

    def process_item(self, item, spider):

        path = urlparse(item["source_file_url"]).path

        item["source_filename"] = os.path.basename(path)

        return item


class BeautifyPipeline:
    def process_item(self, item, spider):
        """Beautify & harmonize projects, titles and petitioners."""

        # Title
        remove_at_start = ["(", "le ", "la "]
        for start in remove_at_start:
            if item["title"].lower().startswith(start):
                item["title"] = item["title"][len(start) :]

        item["title"] = item["title"].strip()
        item["title"] = item["title"][0].capitalize() + item["title"][1:]

        correct_title_words = ["demande", "formulaire", "annexe"]
        if not any(word in item["title"].lower() for word in correct_title_words):

            if item["category_local"] == "Avis conformes":
                item["title"] = "Avis conforme " + item["title"]

            elif item["category_local"] == "Examen au cas par cas et autres décisions":
                item["title"] = "Décision " + item["title"]

            elif item["category_local"] in [
                "Avis rendus sur plans et programmes",
                "Avis rendus sur projets",
            ]:
                item["title"] = "Avis " + item["title"]

        # Project
        if not item["project"] == "Error":
            if item["project"].endswith("))"):
                item["project"] = item["project"][:-1]
            if item["project"].startswith("("):
                item["project"] = item["project"].lstrip("(")
            if "  " in item["project"]:
                item["project"] = item["project"].replace("  ", " ")

            remove_at_start = ["Avis sur le ", "Avis sur la ", "Avis sur "]

            for start in remove_at_start:
                if item["project"].lower().startswith(start.lower()):
                    item["project"] = item["project"][len(start) :]

            item["project"] = item["project"].strip()
            item["project"] = item["project"][0].capitalize() + item["project"][1:]

        # Petitioner
        item["petitioner"] = item["petitioner"].strip()

        remove_at_start = [
            "la ",
            "le ",
            "l'",
            "d'",
            "l’",
            "d’",
            "M. le ",
            "M. le",
        ]
        for start in remove_at_start:
            if item["petitioner"].lower().startswith(start.lower()):
                item["petitioner"] = item["petitioner"][len(start) :]

        item["petitioner"] = item["petitioner"].strip()
        item["petitioner"] = item["petitioner"][0].capitalize() + item["petitioner"][1:]

        if "et de la commune" in item["petitioner"]:
            item["petitioner"] = item["petitioner"].replace(
                "et de la commune", "et commune"
            )

        delete_after = [" en application de", " après examen au cas par cas"]
        for d in delete_after:
            if d in item["petitioner"]:
                item["petitioner"] = item["petitioner"].split(d)[0]

        if re.search("de[A-Z]", item["petitioner"]):
            item["petitioner"] = re.sub(r"de([A-Z])", r"de \1", item["petitioner"])

        item["petitioner"] = (
            item["petitioner"].replace("( ", "(").replace("  ", " ").rstrip(".,")
        )

        return item


class UploadLimitPipeline:
    """Sends the signal to close the spider once the upload limit is attained."""

    def open_spider(self, spider):
        self.number_of_docs = 0

    def process_item(self, item, spider):
        self.number_of_docs += 1

        if spider.upload_limit == 0 or self.number_of_docs < spider.upload_limit + 1:
            return item
        else:
            spider.upload_limit_attained = True
            raise DropItem("Upload limit exceeded.")


class CorrectionsPipeline:
    """Manually correct problematic documents listed in corrections.py"""

    def process_item(self, item, spider):

        url = item["source_file_url"]
        if url in corrections:
            # print(f"Found a correction to do for {url}")

            for k, v in corrections[url].items():
                # print(f"replacing {k} with value {v}")
                item[k] = v

        return item


class MailPipeline:
    """Send scraping run report."""

    def open_spider(self, spider):
        self.items_ok = []
        self.items_with_error = []

    def process_item(self, item, spider):

        if (
            item["project"] == "ERROR"
            or item["petitioner"] == "ERROR"
            or item["decision_date_string"] == "ERROR"
            or item["decision_date"] == "ERROR"
        ):
            self.items_with_error.append(item)
        else:
            self.items_ok.append(item)

        return item

    def close_spider(self, spider):

        def print_item(item, error=False):
            item_string = f"""
            title: {item["title"]}
            project: {item["project"]}
            petitioner: {item["petitioner"]}
            region: {item["region"]}
            category: {item["category"]}
            category_local: {item["category_local"]}
            decision_date: {item["decision_date"]}
            publication_date: {item["publication_date"]}
            source_file_url: {item["source_file_url"]}
            source_page_url: {item["source_page_url"]}
            """

            if error:
                item_string = item_string + f"\nfull_info: {item['full_info']}"

            return item_string

        subject = f"MRAE Scraper Addon Run (Errors: {len(self.items_with_error)} | New: {len(self.items_ok)} )"

        errors_content = f"ERRORS ({len(self.items_with_error)})\n\n" + "\n\n".join(
            [print_item(item, error=True) for item in self.items_with_error]
        )

        ok_content = f"SCRAPED ITEMS ({len(self.items_ok)})\n\n" + "\n\n".join(
            [print_item(item) for item in self.items_ok]
        )

        start_content = f"MRAE Scraper Addon Run {spider.run_id}"

        content = "\n\n".join([start_content, errors_content, ok_content])

        if not spider.dry_run:
            spider.send_mail(subject, content)


class HandleErrorsPipeline:
    """Pass docs with errors to private"""

    def process_item(self, item, spider):

        if (
            item["project"] == "Error"
            or item["petitioner"] == "Error"
            or item["decision_date_string"] == "ERROR"
            or item["decision_date"] == "ERROR"
        ):
            item["access"] = "private"
        else:
            item["access"] = spider.access_level

        return item


class UploadPipeline:
    """Upload document to DocumentCloud & store event data."""

    def process_item(self, item, spider):
        # item["source"] = (
        #     f"Published {item['publication_date']} at {item['publication_time']} on mrae.developpement-durable.gouv.fr"
        # )

        if not spider.dry_run:
            try:
                spider.client.documents.upload(
                    item["source_file_url"],
                    project=spider.target_project,
                    title=item["title"],
                    description=item["project"],
                    source="www.mrae.developpement-durable.gouv.fr",
                    language="fra",
                    access=item["access"],
                    data={
                        "region": item["region"],
                        "category": item["category"],
                        "category_local": item["category_local"],
                        "source_import": "MRAe Scraper",
                        "source_file_url": item["source_file_url"],
                        "source_page_url": item["source_page_url"],
                        "publication_date": item["publication_date"],
                        "publication_time": item["publication_time"],
                        "publication_datetime": item["publication_datetime"],
                        "decision_date": item["decision_date"],
                        # "decision_year": item["decision_year"],
                        # "decision_month": item["decision_month"],
                        # "decision_day": item["decision_day"],
                        "petitioner": item["petitioner"],
                    },
                )
            except Exception as e:
                raise Exception("Upload error").with_traceback(e.__traceback__)
            else:
                # Add to event_data
                now = datetime.datetime.now().isoformat()
                spider.event_data[item["source_file_url"]] = {
                    "headers": item["headers"],
                    "last_seen": now,
                    "run_id": spider.run_id,
                }
                # Save event data
                if spider.run_id:  # only from the web interface
                    spider.store_event_data(spider.event_data)

        return item
