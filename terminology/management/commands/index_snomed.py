from django.core.management.base import BaseCommand
import subprocess

class Command(BaseCommand):
    help = "Index SNOMED data"

    def handle(self, *args, **kwargs):
        self.stdout.write("Running SNOMED Reader...")
        subprocess.run(["python", "-m", "terminology_api.SNOMED.reader"])
        self.stdout.write("Running SNOMED Indexer...")
        subprocess.run(["python", "-m", "terminology_api.SNOMED.indexer"])
        self.stdout.write(self.style.SUCCESS("SNOMED indexing completed"))
