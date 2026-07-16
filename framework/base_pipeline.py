import abc
from datetime import datetime
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery

class BasePipeline(abc.ABC):
    def __init__(self, bucket_name: str, file_name: str):
        self.bucket_name = bucket_name
        self.file_name = file_name
        self.storage_client = storage.Client()
        self.bq_client = bigquery.Client()
        self.bucket = self.storage_client.bucket(bucket_name)
        self.blob = self.bucket.blob(file_name)
        self.df_raw = None
        self.df_final = None
        self.sample_dt = None

    def run(self):
        """The frozen sequential contract flow execution wrapper."""
        self._framework_validate()
        self.download()
        self.prepare()
        self.transform()
        self.load()
        self.verify_load()
        self.archive()
        self.cleanup()

    def _framework_validate(self):
        """Ensures that files residing in archive directories are explicitly bypassed."""
        if "archive/" in self.file_name:
            raise ValueError(f"Skipping file: {self.file_name} (Already archived).")

    def download(self):
        """Streams file payload directly down into memory."""
        if self.file_name.endswith('.csv'):
            self.df_raw = pd.read_csv(self.blob.open("r"))
        elif self.file_name.endswith(('.xlsx', '.xls')):
            self.df_raw = pd.read_excel(self.blob.open("rb"))
        else:
            raise TypeError(f"Unsupported format: {self.file_name}")

    @abc.abstractmethod
    def validate(self):
        """Custom report schema structural assertions hook."""
        pass

    @abc.abstractmethod
    def prepare(self):
        """Custom layout initialization or dynamic matrix unpivot hook."""
        pass

    @abc.abstractmethod
    def transform(self):
        """Custom field mapping, metric cleansing, and enrichment hook."""
        pass

    @abc.abstractmethod
    def get_target_table(self) -> str:
        """Returns target BigQuery destination identifier."""
        pass

    def load(self):
        """Appends the formatted dataset into the BigQuery warehouse target."""
        target = self.get_target_table()
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        load_job = self.bq_client.load_table_from_dataframe(self.df_final, target, job_config=job_config)
        load_job.result()

    def verify_load(self):
        """Placeholder verification layer complying with contract spec."""
        pass

    def archive(self):
        """Commences storage transition utilizing historical partitioning structure."""
        if self.sample_dt is None:
            self.sample_dt = datetime.now()
        year_str = self.sample_dt.strftime("%Y")
        month_str = self.sample_dt.strftime("%m")
        day_str = self.sample_dt.strftime("%d")
        
        # Extracts file name and places archive folder inline with landing configuration
        pure_filename = self.file_name.split("/")[-1]
        
        # Dynamic prefix extraction based on the parent folder layer
        prefix_parts = self.file_name.split("/")[:-1]
        base_prefix = "/".join(prefix_parts) + "/" if prefix_parts else ""
        
        archive_path = f"{base_prefix}archive/year={year_str}/month={month_str}/day={day_str}/{pure_filename}"
        self.bucket.copy_blob(self.blob, self.bucket, archive_path)
        self.blob.delete()

    def cleanup(self):
        """Clears local variable configurations."""
        self.df_raw = None
        self.df_final = None
