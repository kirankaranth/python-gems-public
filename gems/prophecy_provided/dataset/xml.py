from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode, Diagnostic, SeverityLevelEnum
from prophecy.cb.server.base.DatasetBuilderBase import DatasetSpec, DatasetProperties, Component
from prophecy.cb.ui.uispec import *
from prophecy.cb.server.base import WorkflowContext

class xml(DatasetSpec):
    name: str = "xml"
    datasetType: str = "File"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class XMLProperties(DatasetProperties):
        schema: Optional[StructType] = None
        description: Optional[str] = ""
        useSchema: Optional[bool] = True
        path: str = ""
        rowTag: str = ""
        rootTag: Optional[str] = None
        excludeAttribute: Optional[bool] = None
        nullValue: Optional[str] = None
        mode: Optional[str] = None
        attributePrefix: Optional[str] = None
        valueTag: Optional[str] = None
        ignoreSurroundingSpaces: Optional[bool] = None
        ignoreNamespace: Optional[bool] = None
        timestampFormat: Optional[str] = None
        dateFormat: Optional[str] = None
        declaration: Optional[str] = None
        partitionColumns: Optional[List[str]] = None
        writeMode: Optional[str] = None
        compression: Optional[str] = None

    def sourceDialog(self) -> DatasetDialog:
        return DatasetDialog("xml")\
            .addSection("LOCATION", TargetLocation("path")) \
            .addSection(
                "PROPERTIES",
                ColumnsLayout(gap=("1rem"), height=("100%"))
                    .addColumn(
                    ScrollBox().addElement(
                        StackLayout(height=("100%"))
                            .addElement(Checkbox("Enforce schema").bindProperty("useSchema"))
                            .addElement(TextBox("Row Tag", placeholder="ROW").bindProperty("rowTag"))
                            .addElement(
                            StackItem(grow=(1))
                                .addElement(
                                    FieldPicker(height=("100%"))
                                    .addField(Checkbox("Exclude Attributes"), "excludeAttribute")
                                    .addField(TextBox("Null Value"), "nullValue")
                                    .addField(
                                        SelectBox("Parser Mode")
                                        .addOption("Permissive", "PERMISSIVE")
                                        .addOption("Drop Malformed", "DROPMALFORMED")
                                        .addOption("Fail Fast", "FAILFAST"),
                                        "mode"
                                    )
                                    .addField(TextBox("Attribute Prefix"), "attributePrefix")
                                    .addField(TextBox("Value Tag"), "valueTag")
                                    .addField(Checkbox("Ignore Surrounding Spaces"), "ignoreSurroundingSpaces")
                                    .addField(Checkbox("Ignore Namespace"), "ignoreNamespace")
                                    .addField(TextBox("Timestamp Format"), "timestampFormat")
                                    .addField(TextBox("Date Format"), "dateFormat")
                            )
                        )
                    ),
                    "400px"
                )
                .addColumn(SchemaTable("").bindProperty("schema"), "5fr")
            ) \
            .addSection(
                "PREVIEW",
                PreviewTable("").bindProperty("schema")
            )

    def targetDialog(self) -> DatasetDialog:
        return DatasetDialog("xml") \
            .addSection("LOCATION", TargetLocation("path")) \
            .addSection(
                "PROPERTIES",
                ColumnsLayout(gap = "1rem", height = "100%")
                    .addColumn(
                    ScrollBox().addElement(
                        StackLayout(height = "100%")
                        .addElement(TextBox("Row Tag", placeholder = "ROW").bindProperty("rowTag"))
                        .addElement(TextBox("Root Tag", placeholder = "ROWS").bindProperty("rootTag"))
                        .addElement(
                            StackItem(grow = 1)
                            .addElement(
                                FieldPicker(height = "100%")
                                .addField(
                                    SelectBox("Write Mode")
                                    .addOption("error", "error")
                                    .addOption("overwrite", "overwrite")
                                    .addOption("append", "append")
                                    .addOption("ignore", "ignore"),
                                    "writeMode"
                                )
                                .addField(
                                    SchemaColumnsDropdown("Partition Columns")
                                    .withMultipleSelection()
                                    .bindSchema("schema")
                                    .showErrorsFor("partitionColumns"),
                                    "partitionColumns"
                                )
                                .addField(
                                    SelectBox("Compression Codec")
                                    .addOption("none", "none")
                                    .addOption("bzip2", "bzip2")
                                    .addOption("gzip", "gzip")
                                    .addOption("lz4", "lz4")
                                    .addOption("snappy", "snappy")
                                    .addOption("deflate", "deflate"),
                                    "compression"
                                )
                                .addField(TextBox("Null Value"), "nullValue")
                                .addField(TextBox("Attribute Prefix"), "attributePrefix")
                                .addField(TextBox("Value Tag"), "valueTag")
                                .addField(TextBox("Timestamp Format"), "timestampFormat")
                                .addField(TextBox("Date Format"), "dateFormat")
                                .addField(TextBox("XML Declaration"), "declaration")
                            )
                        )
                    ),
                    "400px"
                    )
                    .addColumn(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"), "5fr")
            )

    def validate(self, context: WorkflowContext, component: Component) -> list:
        diagnostics = super(xml, self).validate(context, component)

        if len(component.properties.path) == 0:
            diagnostics.append(
                Diagnostic("properties.path", "path variable cannot be empty [Location]", SeverityLevelEnum.Error)
            )

        from prophecy.cb.util.StringUtils import isBlank
        if isBlank(component.properties.rowTag):
            diagnostics.append(
                Diagnostic("properties.rowTag", "Row tag cannot be empty [Properties]", SeverityLevelEnum.Error)
            )

        if(len(component.ports.inputs) > 0 and len(component.ports.outputs) == 0):
            if component.properties.rootTag is None or isBlank(component.properties.rootTag):
                diagnostics.append(
                    Diagnostic("properties.rootTag", "Root Tag cannot be empty for Target mode [Properties]", SeverityLevelEnum.Error)
                )

        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        return newState

    class XMLFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: xml.XMLProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            reader = spark.read.format("xml") \
                .option("rowTag", self.props.rowTag)

            if self.props.rootTag is not None:
                reader = reader.option("rootTag", self.props.rootTag)
            if self.props.excludeAttribute is not None and self.props.excludeAttribute:
                reader = reader.option("excludeAttribute", self.props.excludeAttribute)
            if self.props.nullValue is not None:
                reader = reader.option("nullValue", self.props.nullValue)
            if self.props.mode is not None:
                reader = reader.option("mode", self.props.mode)
            if self.props.attributePrefix is not None:
                reader = reader.option("attributePrefix", self.props.attributePrefix)
            if self.props.valueTag is not None:
                reader = reader.option("valueTag", self.props.valueTag)
            if self.props.ignoreSurroundingSpaces is not None:
                reader = reader.option("ignoreSurroundingSpaces", self.props.ignoreSurroundingSpaces)
            if self.props.ignoreNamespace is not None:
                reader = reader.option("ignoreNamespace", self.props.ignoreNamespace)
            if self.props.timestampFormat is not None:
                reader = reader.option("timestampFormat", self.props.timestampFormat)
            if self.props.dateFormat is not None:
                reader = reader.option("dateFormat", self.props.dateFormat)

            if self.props.schema is not None and self.props.useSchema:
                reader = reader.schema(self.props.schema)

            return reader.load(self.props.path)

        def targetApply(self, spark: SparkSession, in0: DataFrame):
            writer = in0.write.format("xml") \
                .option("rowTag", self.props.rowTag) \
                .option("rootTag", self.props.rootTag)

            if self.props.writeMode is not None:
                writer = writer.mode(self.props.writeMode)
            if self.props.partitionColumns is not None and len(self.props.partitionColumns) > 0:
                writer = writer.partitionBy(*self.props.partitionColumns)

            if self.props.declaration is not None:
                writer = writer.option("declaration", self.props.declaration)
            if self.props.nullValue is not None:
                writer = writer.option("nullValue", self.props.nullValue)
            if self.props.attributePrefix is not None:
                writer = writer.option("attributePrefix", self.props.attributePrefix)
            if self.props.valueTag is not None:
                writer = writer.option("valueTag", self.props.valueTag)
            if self.props.timestampFormat is not None:
                writer = writer.option("timestampFormat", self.props.timestampFormat)
            if self.props.compression is not None:
                writer = writer.option("compression", self.props.compression)
            if self.props.dateFormat is not None:
                writer = writer.option("dateFormat", self.props.dateFormat)

            writer.save(self.props.path)
