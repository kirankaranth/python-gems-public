from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode, Diagnostic, SeverityLevelEnum
from prophecy.cb.server.base.DatasetBuilderBase import DatasetSpec, DatasetProperties, Component
from prophecy.cb.ui.uispec import *
from prophecy.cb.server.base import WorkflowContext

class fixedformat(DatasetSpec):
    name: str = "fixedformat"
    datasetType: str = "File"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/source-target/file/fixed-format"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class FixedFormatProperties(DatasetProperties):
        schema: Optional[StructType] = None
        description: Optional[str] = ""
        fixedSchemaString: Optional[str] = ""
        fixedSchemaJson: Optional[str] = ""
        path: Optional[str] = ""
        writeMode: Optional[str] = "error"
        skipHeaders: Optional[str] = None
        skipFooters: Optional[str] = None

    def sourceDialog(self) -> DatasetDialog:
        return DatasetDialog("fixedformat") \
            .addSection("LOCATION", TargetLocation("path")) \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                ScrollBox()
                .addElement(
                    StackLayout(gap="1rem")
                    .addElement(
                        StackItem(grow=1)
                        .addElement(
                            FieldPicker(height="100%")
                            .addField(
                                TextArea("Description", 2, placeholder="Dataset description..."),
                                "description",
                                True
                            )
                            .addField(TextBox("Skip header lines").bindPlaceholder("3"), "skipHeaders")
                            .addField(TextBox("Skip footer lines").bindPlaceholder("3"), "skipFooters")
                        )
                    )
                    .addElement(
                        StackLayout()
                        .addElement(TitleElement("Fixed Format Schema"))
                        .addElement(Editor(height="70bh").bindProperty("fixedSchemaString"))
                    )
                ),
                "2fr"
            )
            .addColumn(SchemaTable("").bindProperty("schema"), "5fr")
        ) \
            .addSection(
            "PREVIEW",
            PreviewTable("").bindProperty("schema")
        )

    def targetDialog(self) -> DatasetDialog:
        return DatasetDialog("fixedformat") \
            .addSection("LOCATION", TargetLocation("path")) \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                ScrollBox()
                .addElement(
                    StackLayout(gap="1rem")
                    .addElement(
                        SelectBox("Write Mode")
                        .addOption("error", "error")
                        .addOption("overwrite", "overwrite")
                        .addOption("append", "append")
                        .addOption("ignore", "ignore")
                        .bindProperty("writeMode")
                    )
                    .addElement(
                        StackItem(grow=1)
                        .addElement(
                            TextArea("Description", 2, placeholder="Dataset description..."),
                        )
                    )
                    .addElement(
                        StackLayout()
                        .addElement(TitleElement("Fixed Format Schema"))
                        .addElement(Editor(height="70bh").bindProperty("fixedSchemaString"))
                    )
                ),
                "2fr"
            )
            .addColumn(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"), "3fr")
        )

    def validate(self, context: WorkflowContext, component: Component) -> list:

        import re
        diagnostics = super(fixedformat, self).validate(context, component)

        if len(component.properties.path) == 0:
            diagnostics.append(
                Diagnostic("properties.path", "path variable cannot be empty [Location]", SeverityLevelEnum.Error))

        if (component.properties.skipHeaders is not None) and (
                re.match(r"[0-9]+", component.properties.skipHeaders) is None):
            diagnostics.append(
                Diagnostic("properties.skipHeaders", "Skip header lines must be a non-negative number",
                           SeverityLevelEnum.Error))

        if (component.properties.skipFooters is not None) and (
                re.match(r"[0-9]+", component.properties.skipFooters) is None):
            diagnostics.append(
                Diagnostic("properties.skipFooters", "Skip footer lines must be a non-negative number",
                           SeverityLevelEnum.Error))

        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        from prophecy.utils.transpiler import parse, toSpark
        # TODO: fix this
        newProps = newState.properties
        oldProps = oldState.properties

        print("oldProps =>", oldProps)
        print("newProps =>", newProps)

        try:
            # If schema is updated, reparse
            if oldProps.fixedSchemaString != newProps.fixedSchemaString:
                schemaJson = parse(newProps.fixedSchemaString)
                schemaStruct = toSpark(newProps.fixedSchemaString)
                newState = newState.bindProperties(replace(newState.properties, schema=schemaStruct, fixedSchemaJson=schemaJson))
        except Exception as e:
            print("Exception occurred:", e)
        return newState

    class FixedFormatFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: fixedformat.FixedFormatProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            from prophecy.utils.transpiler import parse
            schema: Optional[str] = None
            if self.props.fixedSchemaString is not None:
                schema = parse(self.props.fixedSchemaString)

            reader = spark.read \
                .option("schema", schema) \
                .format("io.prophecy.libs.FixedFileFormat")

            if self.props.skipHeaders is not None and self.props.skipHeaders != "0":
                reader = reader.option("skip_header_lines", self.props.skipHeaders)

            if self.props.skipFooters is not None and self.props.skipFooters != "0":
                reader = reader.option("skip_footer_lines", self.props.skipFooters)

            return reader.load(self.props.path)

        def targetApply(self, spark: SparkSession, in0: DataFrame):
            from prophecy.utils.transpiler import parse
            schema: Optional[str] = None
            if self.props.fixedSchemaString is not None:
                schema = parse(self.props.fixedSchemaString)

            writer = in0.write
            if self.props.writeMode is not None:
                writer = writer.mode(self.props.writeMode)

            writer \
                .option("schema", schema) \
                .format("io.prophecy.libs.FixedFileFormat") \
                .save(self.props.path)
