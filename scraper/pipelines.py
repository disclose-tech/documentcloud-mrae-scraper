# Item Pipelines

import datetime
import re
import os
from urllib.parse import urlparse
import logging
import json
import hashlib

from scrapy.exceptions import DropItem
from itemadapter import ItemAdapter

from .corrections import corrections
from .log import SilentDropItem
from .departments import department_from_authority, departments_from_project_name


class SpiderPipeline:
    """Base class for pipelines that need access to the spider instance.

    Provides from_crawler() to store spider as self.spider.
    Inherit from this class instead of defining from_crawler() in each pipeline.
    """

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        pipeline.spider = crawler.spider
        return pipeline


class ParseDatePipeline:
    """Parse dates from scraped data."""

    def process_item(self, item):
        """Parses date from the extracted string"""

        # Publication date

        publication_dt = datetime.datetime.strptime(
            item["publication_lastmodified"], "%a, %d %b %Y %H:%M:%S %Z"
        )

        item["publication_date"] = publication_dt.strftime("%Y-%m-%d")
        item["publication_time"] = publication_dt.strftime("%H:%M:%S UTC")
        item["publication_datetime"] = (
            item["publication_date"] + " " + item["publication_time"]
        )

        item["publication_datetime_dcformat"] = (
            publication_dt.isoformat(timespec="microseconds") + "Z"
        )

        return item


class CategoryPipeline:
    """Attributes the final category of the document."""

    def process_item(self, item):

        if "cadrage" in item["title"].lower():
            item["category"] = "Cadrage"

        elif item["category_local"] == "Avis conformes":
            item["category"] = "Cas par cas"

        elif "cas par cas" in item["category_local"].lower():
            item["category"] = "Cas par cas"

        elif item["category_local"] in [
            "Avis rendus sur plans et programmes",
            "Avis rendus sur projets",
        ]:
            item["category"] = "Avis"

        return item


class SourceFilenamePipeline:
    """Adds the source_filename field based on source_file_url."""

    def process_item(self, item):

        path = urlparse(item["source_file_url"]).path

        item["source_filename"] = os.path.basename(path)

        return item


class BeautifyPipeline:
    def process_item(self, item):
        """Beautify & harmonize project names & document titles."""

        # Title

        item["title"] = item["title"].replace(" ", " ").replace("’", "'")
        item["title"] = item["title"].strip()

        # Missing "demande de" / "demande de la"

        if (
            item["title"].startswith("l'établissement public")
            or item["title"].startswith("la commune d")
            or item["title"].startswith("la communauté d'agglo")
        ):
            item["title"] = "Demande de " + item["title"]

        if item["title"].lower().startswith("commune d"):
            item["title"] = "Demande de la " + item["title"]

        remove_at_start = ["(", "le ", "la ", "à la "]
        for start in remove_at_start:
            if item["title"].lower().startswith(start):
                item["title"] = item["title"][len(start) :]

        item["title"] = item["title"].strip()

        # If the title of the doc is only a reference, add "Avis", "Décision" etc at the start for clarity
        if re.match(r"^[\dA-Z -_]+(bis|Bis)?( et [\dA-Z -_]+)?(pdf)?$", item["title"]):

            if item["category_local"] == "Avis conformes":
                item["title"] = "Avis conforme " + item["title"]

            elif item["category_local"] == "Examens au cas par cas et autres décisions":
                item["title"] = "Décision " + item["title"]

            elif item["category_local"] in [
                "Avis rendus sur plans et programmes",
                "Avis rendus sur projets",
            ]:
                item["title"] = "Avis " + item["title"]

        item["title"] = item["title"].strip()
        item["title"] = item["title"][0].capitalize() + item["title"][1:]

        # Project
        if not item["project"] == "Error":

            item["project"] = item["project"].replace(" ", " ").replace("’", "'")
            item["project"] = item["project"].replace("  ", " ")
            item["project"] = item["project"].replace("))", ")")
            item["project"] = item["project"].replace("((", "(")

            remove_at_start = [
                "(",
                "[(",
            ]

            for start in remove_at_start:
                if item["project"].lower().startswith(start.lower()):
                    item["project"] = item["project"][len(start) :]

            item["project"] = item["project"].strip()
            item["project"] = item["project"].rstrip(".}")
            item["project"] = item["project"][0].capitalize() + item["project"][1:]

            # Add missing parenthesis at the end
            item["project"] = re.sub(
                r"\(([02][1-9]|2[AB]|[1345678][0-9]|9[012345]|97[1-8])$",
                r"(\1)",
                item["project"],
            )

        # Authority

        item["authority"] = item["authority"].replace("’", "'")

        return item


class UploadLimitPipeline(SpiderPipeline):
    """Sends the signal to close the spider once the upload limit is attained."""

    def open_spider(self):
        self.number_of_docs = 0

    def process_item(self, item):
        self.number_of_docs += 1

        if (
            self.spider.upload_limit == 0
            or self.number_of_docs < self.spider.upload_limit + 1
        ):
            return item
        else:
            self.spider.upload_limit_attained = True
            raise SilentDropItem("Upload limit exceeded.")


class CorrectionsPipeline:
    """Manually correct problematic documents listed in corrections.py"""

    def process_item(self, item):

        url = item["source_file_url"]
        if url in corrections:
            # print(f"Found a correction to do for {url}")

            for k, v in corrections[url].items():
                # print(f"replacing {k} with value {v}")
                item[k] = v

        return item


class TagDepartmentsPipeline:

    def process_item(self, item):

        authority_department = department_from_authority(item["authority"])

        if authority_department:
            item["departments_sources"] = ["authority"]
            item["departments"] = [authority_department]

        else:

            project_departments = departments_from_project_name(item["project"])

            if project_departments:

                item["departments_sources"] = ["regex"]
                item["departments"] = project_departments

        return item


class HandleErrorsPipeline:
    """Mark docs with error"""

    def process_item(self, item):

        if item["project"].lower() == "error":
            item["error"] = True
        else:
            item["error"] = False

        return item


class ProjectIDPipeline:

    def process_item(self, item):

        project_name = item["project"]
        source_page_url = item["source_page_url"]
        string_to_hash = source_page_url + " " + project_name

        hash_object = hashlib.sha256(string_to_hash.encode())
        hex_dig = hash_object.hexdigest()

        item["project_id"] = hex_dig

        return item


class UploadPipeline(SpiderPipeline):
    """Upload document to DocumentCloud & store event data."""

    def open_spider(self):
        documentcloud_logger = logging.getLogger("documentcloud")
        documentcloud_logger.setLevel(logging.WARNING)

        if not self.spider.dry_run:
            try:
                self.spider.logger.info("Loading event data from DocumentCloud...")
                self.spider.event_data = self.spider.load_event_data()
            except Exception as e:
                raise Exception("Error loading event data").with_traceback(
                    e.__traceback__
                )
                import sys

                sys.exit(1)
        else:
            # Load from json if present
            try:
                with open("event_data.json", "r") as file:
                    self.spider.logger.info(
                        "Loading event data from local JSON file..."
                    )
                    data = json.load(file)
                    self.spider.event_data = data
            except:
                self.spider.event_data = None

        if self.spider.event_data:
            self.spider.logger.info(
                f"Loaded event data ({len(self.spider.event_data)} documents)"
            )
        else:
            self.spider.logger.info("No event data was loaded.")
            self.spider.event_data = {}

    def process_item(self, item):

        data = {
            "authority": item["authority"],
            "category": item["category"],
            "category_local": item["category_local"],
            "event_data_key": item["source_file_url"],
            "source_scraper": f"MRAe Scraper",
            "source_scraper_year": str(item["year"]),
            "source_file_url": item["source_file_url"],
            "source_filename": item["source_filename"],
            "source_page_url": item["source_page_url"],
            "publication_date": item["publication_date"],
            "publication_time": item["publication_time"],
            "publication_datetime": item["publication_datetime"],
            "project_id": item["project_id"],
        }

        adapter = ItemAdapter(item)
        if adapter.get("departments") and adapter.get("departments_sources"):
            data["departments"] = item["departments"]
            data["departments_sources"] = item["departments_sources"]

        if item["error"]:
            data["_tag"] = "hidden"

        try:
            if not self.spider.dry_run:
                self.spider.client.documents.upload(
                    item["source_file_url"],
                    project=self.spider.target_project,
                    title=item["title"],
                    description=item["project"],
                    source=item["source"],
                    publish_at=item["publication_datetime_dcformat"],
                    language="fra",
                    access=self.spider.access_level,
                    data=data,
                )
        except Exception as e:
            raise Exception("Upload error").with_traceback(e.__traceback__)
        else:
            self.spider.logger.debug(
                f"Uploaded {item['source_file_url']} to DocumentCloud"
            )

            last_modified = datetime.datetime.strptime(
                item["publication_lastmodified"], "%a, %d %b %Y %H:%M:%S %Z"
            ).isoformat()
            now = datetime.datetime.now().isoformat(timespec="seconds")

            self.spider.event_data[item["source_file_url"]] = {
                "last_modified": last_modified,
                "last_seen": now,
                "target_year": item["year"],
            }

            if self.spider.run_id:
                self.spider.store_event_data(self.spider.event_data)

        return item

    def close_spider(self):
        """Store event data when the spider closes."""

        if not self.spider.dry_run and self.spider.run_id:
            self.spider.store_event_data(self.spider.event_data)
            self.spider.logger.info(
                f"Uploaded event data ({len(self.spider.event_data)} documents)"
            )

            if self.spider.upload_event_data:
                now = datetime.datetime.now()
                timestamp = now.strftime("%Y%m%d_%H%M")
                filename = f"event_data_IGEDD_{timestamp}.json"

                with open(filename, "w+") as event_data_file:
                    json.dump(self.spider.event_data, event_data_file)
                    self.spider.upload_file(event_data_file)
                self.spider.logger.info(
                    f"Uploaded event data to the Documentcloud interface."
                )

        if not self.spider.run_id:
            with open("event_data.json", "w") as file:
                json.dump(self.spider.event_data, file)
                self.spider.logger.info(
                    f"Saved file event_data.json ({len(self.spider.event_data)} documents)"
                )


class MailPipeline(SpiderPipeline):
    """Send scraping run report."""

    def open_spider(self):
        self.items_ok = []
        self.items_with_error = []

    def process_item(self, item):

        if item["error"] == True:
            self.items_with_error.append(item)
        else:
            self.items_ok.append(item)

        return item

    def close_spider(self):

        def print_item(item, error=False):
            item_string = f"""
            title: {item["title"]}
            project: {item["project"]}
            authority: {item["authority"]}
            category: {item["category"]}
            category_local: {item["category_local"]}
            publication_date: {item["publication_date"]}
            source_file_url: {item["source_file_url"]}
            source_page_url: {item["source_page_url"]}
            """

            if error:
                item_string = item_string + f"\nfull_info: {item['full_info']}"

            return item_string

        if len(self.spider.target_years) == 1:
            year_range_str = str(self.spider.target_years[0])
        else:
            year_range_str = f"{str(self.spider.target_years[0])}-{str(self.spider.target_years[-1])}"

        subject = f"MRAe Scraper {year_range_str} (Errors: {len(self.items_with_error)} | New: {len(self.items_ok)}) [{self.spider.run_name}]"

        errors_content = f"ERRORS ({len(self.items_with_error)})\n\n" + "\n\n".join(
            [print_item(item, error=True) for item in self.items_with_error]
        )

        ok_content = f"SCRAPED ITEMS ({len(self.items_ok)})\n\n" + "\n\n".join(
            [print_item(item) for item in self.items_ok]
        )

        start_content = f"MRAE Scraper Addon Run {self.spider.run_id}"

        content = "\n\n".join([start_content, errors_content, ok_content])

        if not self.spider.dry_run:
            self.spider.send_mail(subject, content)
