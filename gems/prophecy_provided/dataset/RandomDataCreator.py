from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode, Diagnostic, SeverityLevelEnum
from prophecy.cb.server.base.DatasetBuilderBase import DatasetSpec, DatasetProperties, Component
from prophecy.cb.ui.uispec import *
from prophecy.cb.server.base import WorkflowContext

class RandomDataCreator(DatasetSpec):
    name: str = "RandomDataCreator"
    datasetType: str = "File"

    def optimizeCode(self) -> bool:
        return False

    @dataclass(frozen=True)
    class DataCreatorProperties(DatasetProperties):
        numRows: Optional[str] = "1000000"
        dataConfig: Optional[str] = """{ "string(256)":3, "numeric":2 }"""
        description: Optional[str] = "Random Data Creator"
        schema: Optional[StructType] = None

    def sourceDialog(self) -> DatasetDialog:
        return DatasetDialog("DataCreator") \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                ScrollBox().addElement(
                    StackLayout(height="100%")
                    .addElement(
                        StackItem(grow=1).addElement(
                            FieldPicker(height="100%")
                            .addField(
                                TextArea("Description", 2, placeholder="Data description..."),
                                "description",
                                True
                            )
                            .addField(TextBox("Number of Rows"), "numRows", True)
                            .addField(TextArea("Json Config", 25), "dataConfig", True)
                        )
                    )
                ),
                "3fr"
            )
            .addColumn(SchemaTable("").bindProperty("schema"),"5fr")
        )

    def targetDialog(self) -> DatasetDialog:
        return DatasetDialog("DataCreator") \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                ColumnsLayout(gap="1rem", height="100%")
                .addColumn(
                    ScrollBox().addElement(
                        StackLayout(height="100%")
                        .addElement(
                            StackItem(grow=1).addElement(
                                FieldPicker(height="100%")
                                .addField(
                                    TextArea("Description", 2, placeholder="Data description..."),
                                    "description",
                                    True
                                )
                                .addField(TextBox("Number of Rows"), "numRows", True)
                            )
                        )
                    ),
                    "2fr"
                )
                .addColumn(
                    StackLayout(height=("100%")).addElement(
                        TextArea("Json Config", 25).bindProperty("dataConfig")), "2fr"),
                "5fr"
            )
        )

    def validate(self, context: WorkflowContext, component: Component) -> list:
        diagnostics = []
        if int(component.properties.numRows) <= 0:
            diagnostics.append(
                Diagnostic("properties.numRows", "Number of Rows variable cannot be empty [Location]", SeverityLevelEnum.Error),
            )
        try:
            import json
            json.loads(component.properties.dataConfig)  # To Validate if json_loads would succeed:
        except Exception as e:
            diagnostics.append(
                Diagnostic("properties.dataConfig", "Config needs to be a valid JSON [Properties]", SeverityLevelEnum.Error),
            )
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        return newState

    class DataCreatorFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: RandomDataCreator.DataCreatorProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            from prophecy.random_data_creator import create_data_batch  # noqa
            import json
            config = json.loads(self.props.dataConfig)
            data = spark.range(int(self.props.numRows))
            final_col_name_list = create_data_batch(config)
            data = data.select(*final_col_name_list)
            return data

        def targetApply(self, spark: SparkSession, in0: DataFrame):
            pass