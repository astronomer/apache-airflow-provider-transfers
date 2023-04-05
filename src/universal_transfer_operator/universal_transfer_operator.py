from __future__ import annotations

from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context

from universal_transfer_operator.constants import TransferMode
from universal_transfer_operator.data_providers import create_dataprovider, get_dataprovider_options_class
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Table
from universal_transfer_operator.integrations import (
    get_conn_id,
    get_ingestor_option_class,
    get_transfer_integration,
)
from universal_transfer_operator.integrations.base import TransferIntegrationOptions


class UniversalTransferOperator(BaseOperator):
    """
    Transfers all the data that could be read from the source Dataset into the destination Dataset. From a DAG author
    standpoint, all transfers would be performed through the invocation of only the Universal Transfer Operator.

    :param source_dataset: Source dataset to be transferred.
    :param destination_dataset: Destination dataset to be transferred to.
    :param transfer_params: kwargs to be used by method involved in transfer flow.
    :param transfer_mode: Use transfer_mode TransferMode; native, non-native or thirdparty.
    :param if_exists: Overwrite file if exists. Default False.

    :return: returns the destination dataset
    """

    def __init__(
        self,
        *,
        source_dataset: Table | File,
        destination_dataset: Table | File,
        transfer_params: TransferIntegrationOptions | dict | None = None,
        transfer_mode: TransferMode = TransferMode.NONNATIVE,
        **kwargs,
    ) -> None:
        transfer_params = transfer_params or {}
        self.source_dataset = source_dataset
        self.destination_dataset = destination_dataset
        self.transfer_mode = transfer_mode
        # TODO: revisit names of transfer_mode
        self.transfer_params: TransferIntegrationOptions = (
            self._get_options_class(dataset=destination_dataset, transfer_params=transfer_params)(
                **transfer_params
            )
            if isinstance(transfer_params, dict)
            else transfer_params
        )
        super().__init__(**kwargs)

    def execute(self, context: Context) -> Any:  # skipcq: PYL-W0613
        if self.transfer_mode == TransferMode.THIRDPARTY:
            transfer_integration = get_transfer_integration(self.transfer_params)
            return transfer_integration.transfer_job(self.source_dataset, self.destination_dataset)

        source_dataprovider = create_dataprovider(
            dataset=self.source_dataset,
            transfer_params=self.transfer_params,
            transfer_mode=self.transfer_mode,
        )

        destination_dataprovider = create_dataprovider(
            dataset=self.destination_dataset,
            transfer_params=self.transfer_params,
            transfer_mode=self.transfer_mode,
        )

        destination_references = []
        for source_data in source_dataprovider.read():
            destination_references.append(destination_dataprovider.write(source_data))
        return destination_references

    def _get_options_class(
        self, transfer_params: TransferIntegrationOptions | dict, dataset: Table | File
    ) -> type[TransferIntegrationOptions]:
        """
        Get option class based on transfer type
        """
        if self.transfer_mode == TransferMode.THIRDPARTY:
            conn_id = get_conn_id(transfer_params=transfer_params)
            return get_ingestor_option_class(conn_id=conn_id)
        else:
            return get_dataprovider_options_class(dataset=dataset)
