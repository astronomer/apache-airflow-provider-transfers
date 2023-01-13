from __future__ import annotations

from typing import Any

import attr
from airflow.models import BaseOperator
from airflow.utils.context import Context

from universal_transfer_operator.constants import TransferMode
from universal_transfer_operator.data_providers import create_dataprovider
from universal_transfer_operator.datasets.base import Dataset
from universal_transfer_operator.integrations import get_transfer_integration
from universal_transfer_operator.utils import TransferParameters


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
        source_dataset: Dataset,
        destination_dataset: Dataset,
        transfer_params: TransferParameters = attr.field(
            factory=TransferParameters,
            converter=lambda val: TransferParameters(**val) if isinstance(val, dict) else val,
        ),
        transfer_mode: TransferMode = TransferMode.NONNATIVE,
        **kwargs,
    ) -> None:
        self.source_dataset = source_dataset
        self.destination_dataset = destination_dataset
        self.transfer_mode = transfer_mode
        # TODO: revisit names of transfer_mode
        self.transfer_params = transfer_params
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

        with source_dataprovider.read() as source_data:
            destination_data = destination_dataprovider.write(source_data)

        return destination_data
