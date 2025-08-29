from django.core.management.base import BaseCommand
import subprocess

class Command(BaseCommand):
    help = "Index both SNOMED and LOINC data"

    def handle(self, *args, **kwargs):
        self.stdout.write("Starting SNOMED + LOINC indexing...")

        subprocess.run(["python", "-m", "terminology_api.SNOMED.reader"])
        subprocess.run(["python", "-m", "terminology_api.SNOMED.indexer"])

        subprocess.run(["python", "-m", "terminology_api.LOINC.reader"])
        subprocess.run(["python", "-m", "terminology_api.LOINC.indexer"])

        self.stdout.write(self.style.SUCCESS("SNOMED + LOINC indexing completed"))
