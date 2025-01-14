from dataclasses import dataclass


@dataclass
class AzureStorage:
    """
    For some useful constants
    """

    AZURE_STORAGE_ACCOUNT_URL: str = "https://cfaazurebatchprd.blob.core.windows.net/"
    AZURE_CONTAINER_NAME: str = "rt-epinow2-config"
    SCOPE_URL: str = "https://cfaazurebatchprd.blob.core.windows.net/.default"
