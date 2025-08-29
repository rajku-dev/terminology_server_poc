from django.core.management.base import BaseCommand
import subprocess

class Command(BaseCommand):
    help = "Index LOINC data"

    def handle(self, *args, **kwargs):
        self.stdout.write("Running LOINC Reader...")
        subprocess.run(["python", "-m", "terminology_api.LOINC.reader"])
        self.stdout.write("Running LOINC Indexer...")
        subprocess.run(["python", "-m", "terminology_api.LOINC.indexer"])
        self.stdout.write(self.style.SUCCESS("LOINC indexing completed"))
