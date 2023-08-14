from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode, Diagnostic, SeverityLevelEnum
from prophecy.cb.server.base.DatasetBuilderBase import DatasetSpec, DatasetProperties, Component
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.NumberUtils import parseFloat
from prophecy.cb.server.base import WorkflowContext

class json(DatasetSpec):
    name: str = "json"
    datasetType: str = "File"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/source-target/file/json"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class JsonProperties(DatasetProperties):
        schema: Optional[StructType] = None
        description: Optional[str] = ""
        useSchema: Optional[bool] = True
        path: str = ""
        primitivesAsString: Optional[bool] = None
        prefersDecimal: Optional[bool] = None
        allowComments: Optional[bool] = None
        allowUnquotedFieldNames: Optional[bool] = None
        allowSingleQuotes: Optional[bool] = None
        allowNumericLeadingZeros: Optional[bool] = None
        allowBackslashEscapingAnyCharacter: Optional[bool] = None
        allowUnquotedControlChars: Optional[bool] = None
        mode: Optional[str] = None
        columnNameOfCorruptRecord: Optional[str] = None
        dateFormat: Optional[str] = None
        timestampFormat: Optional[str] = None
        multiLine: Optional[bool] = None
        lineSep: Optional[str] = None
        samplingRatio: Optional[str] = None
        dropFieldIfAllNull: Optional[bool] = None
        writeMode: Optional[str] = None
        compression: Optional[str] = None
        partitionColumns: Optional[List[str]] = None
        encoding: Optional[str] = None
        ignoreNullFields: Optional[bool] = None
        recursiveFileLookup: Optional[bool] = None

    def sourceDialog(self) -> DatasetDialog:
        return DatasetDialog("json") \
            .addSection("LOCATION", TargetLocation("path")) \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap=("1rem"), height=("100%"))
                .addColumn(
                ScrollBox().addElement(
                    StackLayout(height=("100%"))
                        .addElement(
                        StackItem(grow=(1)).addElement(
                            FieldPicker(height=("100%"))
                                .addField(
                                TextArea("Description", 2, placeholder="Dataset description..."),
                                "description",
                                True
                            )
                                .addField(Checkbox("Use user-defined schema"), "useSchema", True)
                                .addField(Checkbox("Parse Multi-line records"), "multiLine")
                                .addField(TextBox("New line separator").bindPlaceholder(""), "lineSep")
                                .addField(Checkbox("Infer primitive values as string type"), "primitivesAsString")
                                .addField(Checkbox("Infer floating-point values as decimal or double type"),
                                          "prefersDecimal")
                                .addField(Checkbox("Ignore Java/C++ style comment in Json records"), "allowComments")
                                .addField(Checkbox("Allow unquoted field names"), "allowUnquotedFieldNames")
                                .addField(Checkbox("Allow single quotes"), "allowSingleQuotes")
                                .addField(Checkbox("Allow leading zero in numbers"), "allowNumericLeadingZeros")
                                .addField(Checkbox("Allow Backslash escaping"), "allowBackslashEscapingAnyCharacter")
                                .addField(Checkbox("Allow unquoted control characters in JSON string"),
                                          "allowUnquotedControlChars")
                                .addField(
                                SelectBox("Mode to deal with corrupt records")
                                    .addOption("PERMISSIVE", "PERMISSIVE")
                                    .addOption("DROPMALFORMED", "DROPMALFORMED")
                                    .addOption("FAILFAST", "FAILFAST"),
                                "mode"
                            )
                                .addField(
                                TextBox("Column name of a corrupt record")
                                    .bindPlaceholder(""),
                                "columnNameOfCorruptRecord"
                            )
                                .addField(TextBox("Date Format String").bindPlaceholder(""), "dateFormat")
                                .addField(TextBox("Timestamp Format String").bindPlaceholder(""), "timestampFormat")
                                .addField(
                                TextBox("Sampling ratio for schema inferring")
                                    .bindPlaceholder(""),
                                "samplingRatio"
                            )
                                .addField(
                                Checkbox("Ignore column with all null values during schema inferring"),
                                "dropFieldIfAllNull"
                            )
                                .addField(Checkbox("Recursive File Lookup"), "recursiveFileLookup")
                        )
                    )
                ),
                "auto"
            )
                .addColumn(SchemaTable("").bindProperty("schema"), "5fr")
        ) \
            .addSection(
            "PREVIEW",
            PreviewTable("").bindProperty("schema")
        )

    def targetDialog(self) -> DatasetDialog:
        return DatasetDialog("json") \
            .addSection("LOCATION", TargetLocation("path")) \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap=("1rem"), height=("100%"))
                .addColumn(
                ScrollBox().addElement(
                    StackLayout(height=("100%")).addElement(
                        FieldPicker(height=("100%"))
                            .addField(
                            TextArea("Description", 2, placeholder="Dataset description..."),
                            "description",
                            True
                        )
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
                            .addField(TextBox("Date Format String").bindPlaceholder(""), "dateFormat")
                            .addField(TextBox("Timestamp Format String").bindPlaceholder(""), "timestampFormat")
                            .addField(TextBox("Encoding").bindPlaceholder(""), "encoding")
                            .addField(TextBox("Line Separator").bindPlaceholder(""), "lineSep")
                            .addField(Checkbox("Ignore null fields"), "ignoreNullFields")
                    )
                ),
                "auto"
            )
                .addColumn(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"), "5fr")
        )

    def validate(self, context: WorkflowContext, component: Component) -> list:
        diagnostics = super(json, self).validate(context, component)
        if len(component.properties.path) == 0:
            diagnostics.append(
                Diagnostic("properties.path", "path variable cannot be empty [Location]", SeverityLevelEnum.Error))
        if component.properties.samplingRatio is not None:
            floatValue = parseFloat(component.properties.samplingRatio)
            if floatValue is None:
                diagnostics.append(
                    Diagnostic("properties.samplingRatio", "Invalid sampling ratio [Properties]",
                               SeverityLevelEnum.Error))
            elif 0.0 < floatValue <= 1.0:
                return diagnostics
            else:
                diagnostics.append(
                    Diagnostic("properties.samplingRatio", "Sampling Ratio has to be between (0.0, 1.0] [Properties]",
                               SeverityLevelEnum.Error))
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        return newState

    class JsonFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: json.JsonProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            reader = spark.read.format("json")
            if self.props.multiLine is not None:
                reader = reader.option("multiLine", self.props.multiLine)
            if self.props.lineSep is not None:
                reader = reader.option("lineSep", self.props.lineSep)
            if self.props.primitivesAsString is not None:
                reader = reader.option("primitivesAsString", self.props.primitivesAsString)
            if self.props.prefersDecimal is not None:
                reader = reader.option("prefersDecimal", self.props.prefersDecimal)
            if self.props.allowComments is not None:
                reader = reader.option("allowComments", self.props.allowComments)
            if self.props.allowUnquotedFieldNames is not None:
                reader = reader.option("allowUnquotedFieldNames", self.props.allowUnquotedFieldNames)
            if self.props.allowSingleQuotes is not None:
                reader = reader.option("allowSingleQuotes", self.props.allowSingleQuotes)
            if self.props.allowNumericLeadingZeros is not None:
                reader = reader.option("allowNumericLeadingZeros", self.props.allowNumericLeadingZeros)
            if self.props.allowBackslashEscapingAnyCharacter is not None:
                reader = reader.option("allowBackslashEscapingAnyCharacter",
                                       self.props.allowBackslashEscapingAnyCharacter)
            if self.props.allowUnquotedControlChars is not None:
                reader = reader.option("allowUnquotedControlChars", self.props.allowUnquotedControlChars)
            if self.props.mode is not None:
                reader = reader.option("mode", self.props.mode)
            if self.props.columnNameOfCorruptRecord is not None:
                reader = reader.option("columnNameOfCorruptRecord", self.props.columnNameOfCorruptRecord)
            if self.props.dateFormat is not None:
                reader = reader.option("dateFormat", self.props.dateFormat)
            if self.props.timestampFormat is not None:
                reader = reader.option("timestampFormat", self.props.timestampFormat)
            if self.props.samplingRatio is not None:
                reader = reader.option("samplingRatio", self.props.samplingRatio)
            if self.props.dropFieldIfAllNull is not None:
                reader = reader.option("dropFieldIfAllNull", self.props.dropFieldIfAllNull)
            if self.props.recursiveFileLookup is not None:
                reader = reader.option("recursiveFileLookup", self.props.recursiveFileLookup)
            if self.props.schema is not None and self.props.useSchema:
                reader = reader.schema(self.props.schema)
            return reader.load(self.props.path)

        def targetApply(self, spark: SparkSession, in0: DataFrame):
            writer = in0.write.format("json")

            if self.props.writeMode is not None:
                writer = writer.mode(self.props.writeMode)
            if self.props.compression is not None:
                writer = writer.option("compression", self.props.compression)
            if self.props.dateFormat is not None:
                writer = writer.option("dateFormat", self.props.dateFormat)
            if self.props.timestampFormat is not None:
                writer = writer.option("timestampFormat", self.props.timestampFormat)
            if self.props.encoding is not None:
                writer = writer.option("encoding", self.props.encoding)
            if self.props.lineSep is not None:
                writer = writer.option("lineSep", self.props.lineSep)
            if self.props.ignoreNullFields is not None:
                writer = writer.option("ignoreNullFields", self.props.ignoreNullFields)
            if self.props.partitionColumns is not None and len(self.props.partitionColumns) > 0:
                writer = writer.partitionBy(*self.props.partitionColumns)
            writer.save(self.props.path)
