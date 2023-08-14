from pyspark.sql import SparkSession, DataFrame

from pyspark.sql.types import StructType
from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode
from prophecy.cb.server.base.DatasetBuilderBase import (
    DatasetSpec,
    DatasetProperties,
    Component,
)
from prophecy.cb.ui.uispec import *
from prophecy.cb.server.base import WorkflowContext


class bigquery(DatasetSpec):
    name: str = "bigquery"
    datasetType: str = "Warehouse"
    docUrl: str = (
        "https://docs.prophecy.io/low-code-spark/gems/source-target/warehouse/bigquery"
    )

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class BigQueryProperties(DatasetProperties):
        schema: Optional[StructType] = None
        description: Optional[str] = ""
        credType: str = "none"
        credentialsFile: Optional[str] = None
        secretsVariable: Optional[str] = None
        isBase64: bool = False
        parentProject: Optional[str] = None
        table: Optional[str] = None
        datasetOption: Optional[str] = None
        project: Optional[str] = None
        maxParallelism: Optional[str] = None
        preferredMinParallelism: Optional[str] = None
        viewsEnabled: Optional[bool] = None
        materializationProject: Optional[str] = None
        materializationDataset: Optional[str] = None
        materializationExpirationTimeInMinutes: Optional[str] = None
        readDataFormat: Optional[str] = None
        optimizedEmptyProjection: Optional[bool] = None
        pushAllFilters: Optional[bool] = None
        bigQueryJobLabel: Optional[str] = None
        bigQueryTableLabel: Optional[str] = None
        traceApplicationName: Optional[str] = None
        traceJobId: Optional[str] = None
        createDisposition: Optional[str] = None
        writeMethod: Optional[str] = None
        temporaryGcsBucket: Optional[str] = None
        persistentGcsBucket: Optional[str] = None
        persistentGcsPath: Optional[str] = None
        intermediateFormat: Optional[str] = None
        useAvroLogicalTypes: Optional[bool] = None
        datePartition: Optional[str] = None
        partitionField: Optional[str] = None
        partitionExpirationMs: Optional[str] = None
        partitionType: Optional[str] = None
        clusteredFields: Optional[List[str]] = None
        allowFieldAddition: Optional[bool] = None
        allowFieldRelaxation: Optional[bool] = None
        proxyAddress: Optional[str] = None
        proxyUsername: Optional[str] = None
        proxyPassword: Optional[str] = None
        httpMaxRetry: Optional[str] = None
        httpConnectTimeout: Optional[str] = None
        httpReadTimeout: Optional[str] = None
        arrowCompressionCodec: Optional[str] = None
        cacheExpirationTimeInMinutes: Optional[str] = None
        enableModeCheckForSchemaFields: Optional[bool] = None
        enableListInference: Optional[bool] = None
        createReadSessionTimeoutInSeconds: Optional[str] = None
        datetimeZoneId: Optional[str] = None
        queryJobPriority: Optional[str] = None
        gcpAccessToken: Optional[str] = None
        writeMode: Optional[str] = None

    def sourceDialog(self) -> DatasetDialog:
        return (
            DatasetDialog("BigQuery")
                .addSection(
                "LOCATION",
                ColumnsLayout(gap="1rem", height="100%").addColumn(
                    StackLayout(direction="vertical", gap="1rem")
                        .addElement(TitleElement(title="Parent Project Name"))
                        .addElement(
                        TextBox("Parent Project Name")
                            .bindPlaceholder("bigquery-test")
                            .bindProperty("parentProject")
                    )
                        .addElement(
                        TextBox("Table Name")
                            .bindPlaceholder("customer_tbl")
                            .bindProperty("table")
                    )
                        .addElement(
                        StackLayout(direction="vertical", gap="1rem")
                            .addElement(
                            RadioGroup("Credentials", defaultValue="none")
                                .addOption("None", "none")
                                .addOption("JSON Credentials Filepath", "credentialsFile")
                                .addOption("DataBricks secrets", "databricksSecrets")
                                .bindProperty("credType")
                        )
                            .addElement(
                            Condition()
                                .ifEqual(
                                PropExpr("component.properties.credType"),
                                StringExpr("credentialsFile"),
                            )
                                .then(
                                TextBox("Credentials file path")
                                    .bindPlaceholder("bigquery key JSON path")
                                    .bindProperty("credentialsFile")
                            )
                                .otherwise(
                                Condition()
                                    .ifEqual(
                                    PropExpr("component.properties.credType"),
                                    StringExpr("databricksSecrets"),
                                )
                                    .then(
                                    StackLayout(direction="vertical", gap="1rem")
                                        .addElement(
                                        TextBox("Configuration variable name")
                                            .bindPlaceholder("${config_test_secret}")
                                            .bindProperty("secretsVariable")
                                    )
                                        .addElement(
                                        Checkbox(
                                            "Is secret base64 encoded"
                                        ).bindProperty("isBase64")
                                    )
                                )
                            )
                        )
                    )
                ),
            )
                .addSection(
                "PROPERTIES",
                ColumnsLayout(gap="1rem", height="100%")
                    .addColumn(
                    ScrollBox().addElement(
                        StackLayout(height="100%").addElement(
                            StackItem(grow=1).addElement(
                                FieldPicker(height="100%")
                                    .addField(
                                    TextArea(
                                        "Description",
                                        2,
                                        placeholder="Dataset description...",
                                    ),
                                    "description",
                                    True,
                                )
                                    .addField(
                                    TextBox("Project Name").bindPlaceholder(""),
                                    "project",
                                )
                                    .addField(
                                    TextBox("Dataset Name").bindPlaceholder(""),
                                    "datasetOption",
                                )
                                    .addField(
                                    TextBox("Maximum partitions").bindPlaceholder(""),
                                    "maxParallelism",
                                )
                                    .addField(
                                    TextBox("Minimum partitions").bindPlaceholder(""),
                                    "preferredMinParallelism",
                                )
                                    .addField(
                                    Checkbox("Enables read views"),
                                    "viewsEnabled",
                                )
                                    .addField(
                                    TextBox(
                                        "MaterializedView projectID"
                                    ).bindPlaceholder(""),
                                    "materializationProject",
                                )
                                    .addField(
                                    TextBox("MaterializedView dataset").bindPlaceholder(
                                        ""
                                    ),
                                    "materializationDataset",
                                )
                                    .addField(
                                    TextBox(
                                        "Materialized expiration time in min's"
                                    ).bindPlaceholder(""),
                                    "materializationExpirationTimeInMinutes",
                                )
                                    .addField(
                                    SelectBox("Read dataformat")
                                        .addOption("Avro", "AVRO")
                                        .addOption("Arrow", "ARROW"),
                                    "readDataFormat",
                                )
                                    .addField(
                                    Checkbox("Enable optimize-empty-projection"),
                                    "optimizedEmptyProjection",
                                )
                                    .addField(
                                    Checkbox("Enable push-all-filters"),
                                    "pushAllFilters",
                                )
                                    .addField(
                                    TextBox("Additional Job Labels").bindPlaceholder(
                                        ""
                                    ),
                                    "bigQueryJobLabel",
                                )
                                    .addField(
                                    TextBox(
                                        "Traceability Application Name"
                                    ).bindPlaceholder(""),
                                    "traceApplicationName",
                                )
                                    .addField(
                                    TextBox("Traceability job ID").bindPlaceholder(""),
                                    "traceJobId",
                                )
                                    .addField(
                                    TextBox("Proxy URL").bindPlaceholder(""),
                                    "proxyAddress",
                                )
                                    .addField(
                                    TextBox("Proxy username").bindPlaceholder(""),
                                    "proxyUsername",
                                )
                                    .addField(
                                    TextBox("Proxy password").bindPlaceholder(""),
                                    "proxyPassword",
                                )
                                    .addField(
                                    TextBox("Maximum HTTP retries").bindPlaceholder(""),
                                    "httpMaxRetry",
                                )
                                    .addField(
                                    TextBox(
                                        "HTTP Connection timeout in MSec's"
                                    ).bindPlaceholder(""),
                                    "httpConnectTimeout",
                                )
                                    .addField(
                                    TextBox(
                                        "HTTP Read timeout in MSec's"
                                    ).bindPlaceholder(""),
                                    "httpReadTimeout",
                                )
                                    .addField(
                                    SelectBox("Arrow Compression Codec")
                                        .addOption("None", "COMPRESSION_UNSPECIFIED")
                                        .addOption("Zstandard", "ZSTD")
                                        .addOption("LZ4 Frame", "LZ4_FRAME"),
                                    "arrowCompressionCodec",
                                )
                                    .addField(
                                    TextBox(
                                        "Cache expiration time in min's"
                                    ).bindPlaceholder(""),
                                    "cacheExpirationTimeInMinutes",
                                )
                                    .addField(
                                    TextBox(
                                        "Cache read session timeout in sec's"
                                    ).bindPlaceholder(""),
                                    "createReadSessionTimeoutInSeconds",
                                )
                                    .addField(
                                    TextBox("GCP Access Token").bindPlaceholder(
                                        "gcpAccessToken"
                                    ),
                                    "gcpAccessToken",
                                )
                                    .addField(
                                    TextBox(
                                        "Conversation datetime zone ID"
                                    ).bindPlaceholder("UTC"),
                                    "datetimeZoneId",
                                )
                                    .addField(
                                    SelectBox("Job query priority")
                                        .addOption("Batch", "BATCH")
                                        .addOption("Interactive", "INTERACTIVE"),
                                    "queryJobPriority",
                                )
                            )
                        )
                    ),
                    "auto",
                )
                    .addColumn(SchemaTable("").bindProperty("schema"), "5fr"),
            )
                .addSection("PREVIEW", PreviewTable("").bindProperty("schema"))
        )

    def targetDialog(self) -> DatasetDialog:
        return (
            DatasetDialog("BigQuery")
                .addSection(
                "LOCATION",
                ColumnsLayout().addColumn(
                    StackLayout(direction="vertical", gap="1rem")
                        .addElement(TitleElement(title="Parent Project Name"))
                        .addElement(
                        TextBox("Parent Project Name")
                            .bindPlaceholder("bigquery-test")
                            .bindProperty("parentProject")
                    )
                        .addElement(
                        TextBox("Table Name")
                            .bindPlaceholder("customer_tbl")
                            .bindProperty("table")
                    )
                        .addElement(
                        StackLayout(direction="vertical", gap="1rem")
                            .addElement(
                            RadioGroup("Credentials", defaultValue="none")
                                .addOption("None", "none")
                                .addOption("JSON Credentials Filepath", "credentialsFile")
                                .addOption(
                                "DataBricks secrets variable", "databricksSecrets"
                            )
                                .bindProperty("credType")
                        )
                            .addElement(
                            Condition()
                                .ifEqual(
                                PropExpr("component.properties.credType"),
                                StringExpr("credentialsFile"),
                            )
                                .then(
                                TextBox("Credentials file path")
                                    .bindPlaceholder("bigquery key JSON path")
                                    .bindProperty("credentialsFile")
                            )
                                .otherwise(
                                Condition()
                                    .ifEqual(
                                    PropExpr("component.properties.credType"),
                                    StringExpr("databricksSecrets"),
                                )
                                    .then(
                                    StackLayout(direction="vertical", gap="1rem")
                                        .addElement(
                                        TextBox("Configuration variable name")
                                            .bindPlaceholder("${config_test_secret}")
                                            .bindProperty("secretsVariable")
                                    )
                                        .addElement(
                                        Checkbox(
                                            "Is secret base64 encoded"
                                        ).bindProperty("isBase64")
                                    )
                                )
                            )
                        )
                    )
                ),
            )
                .addSection(
                "PROPERTIES",
                ColumnsLayout(gap="1rem", height="100%")
                    .addColumn(
                    ScrollBox().addElement(
                        StackLayout(height="100%").addElement(
                            StackItem(grow=1).addElement(
                                FieldPicker(height="100%")
                                    .addField(
                                    TextArea(
                                        "Description",
                                        2,
                                        placeholder="Dataset description...",
                                    ),
                                    "description",
                                    True,
                                )
                                    .addField(
                                    TextBox("Project Name").bindPlaceholder(""),
                                    "project",
                                )
                                    .addField(
                                    TextBox("Dataset Name").bindPlaceholder(""),
                                    "datasetOption",
                                )
                                    .addField(
                                    TextBox("Table labels").bindPlaceholder(""),
                                    "bigQueryTableLabel",
                                )
                                    .addField(
                                    SelectBox("Disposition creation")
                                        .addOption(
                                        "Create table if not exists", "CREATE_IF_NEEDED"
                                    )
                                        .addOption("Don't create table", "CREATE_NEVER"),
                                    "createDisposition",
                                )
                                    .addField(
                                    SelectBox("Write Mode")
                                        .addOption("overwrite", "overwrite")
                                        .addOption("append", "append"),
                                    "writeMode",
                                )
                                    .addField(
                                    SelectBox("Write Method")
                                        .addOption("Use storage write API", "direct")
                                        .addOption(
                                        "Write first to GCS and Load", "indirect"
                                    ),
                                    "writeMethod",
                                )
                                    .addField(
                                    TextBox("Temporary GCS Bucket").bindPlaceholder(""),
                                    "temporaryGcsBucket",
                                )
                                    .addField(
                                    TextBox("Persistent GCS Bucket").bindPlaceholder(
                                        ""
                                    ),
                                    "persistentGcsBucket",
                                )
                                    .addField(
                                    TextBox("Persistent GCS Path").bindPlaceholder(""),
                                    "persistentGcsPath",
                                )
                                    .addField(
                                    SelectBox("Intermediate dataformat")
                                        .addOption("Parquet", "parquet")
                                        .addOption("ORC", "orc")
                                        .addOption("Avro", "avro"),
                                    "intermediateFormat",
                                )
                                    # .addField(Checkbox("Enable Avro-logical-type"), "useAvroLogicalTypes")
                                    .addField(
                                    TextBox("Date partition").bindPlaceholder(
                                        "20220331"
                                    ),
                                    "datePartition",
                                )
                                    .addField(
                                    TextBox("Partition field").bindPlaceholder(""),
                                    "partitionField",
                                )
                                    .addField(
                                    TextBox(
                                        "Partition expiration MSec's"
                                    ).bindPlaceholder(""),
                                    "partitionExpirationMs",
                                )
                                    .addField(
                                    SelectBox("Partition type of the field")
                                        .addOption("Hour", "HOUR")
                                        .addOption("Day", "DAY")
                                        .addOption("Month", "MONTH")
                                        .addOption("Year", "YEAR"),
                                    "partitionType",
                                )
                                    .addField(
                                    SchemaColumnsDropdown("Cluster fields")
                                        .withMultipleSelection()
                                        .bindSchema("schema")
                                        .showErrorsFor("clusteredFields"),
                                    "clusteredFields",
                                )
                                    .addField(
                                    Checkbox("Enable allow-field-addition"),
                                    "allowFieldAddition",
                                )
                                    .addField(
                                    Checkbox("Enable allow-field-relaxation"),
                                    "allowFieldRelaxation",
                                )
                                    .addField(
                                    TextBox("Proxy URL").bindPlaceholder(""),
                                    "proxyAddress",
                                )
                                    .addField(
                                    TextBox("Proxy username").bindPlaceholder(""),
                                    "proxyUsername",
                                )
                                    .addField(
                                    TextBox("Proxy password").bindPlaceholder(""),
                                    "proxyPassword",
                                )
                                    .addField(
                                    TextBox("Maximum HTTP retries").bindPlaceholder(""),
                                    "httpMaxRetry",
                                )
                                    .addField(
                                    TextBox(
                                        "HTTP Connection timeout in MSec's"
                                    ).bindPlaceholder(""),
                                    "httpConnectTimeout",
                                )
                                    .addField(
                                    Checkbox("Enable mode-check-for-schema-fields"),
                                    "enableModeCheckForSchemaFields",
                                )
                                    .addField(
                                    Checkbox("Enable list-interface"),
                                    "enableListInference",
                                )
                                    .addField(
                                    TextBox(
                                        "Conversation datetime zone ID"
                                    ).bindPlaceholder("UTC"),
                                    "datetimeZoneId",
                                )
                                    .addField(
                                    TextBox("GCP Access Token").bindPlaceholder(
                                        "gcpAccessToken"
                                    ),
                                    "gcpAccessToken",
                                )
                                    .addField(
                                    SelectBox("Job query priority")
                                        .addOption("Batch", "BATCH")
                                        .addOption("Interactive", "INTERACTIVE"),
                                    "queryJobPriority",
                                )
                            )
                        )
                    ),
                    "auto",
                )
                    .addColumn(SchemaTable("").bindProperty("schema"), "5fr"),
            )
                .addSection("PREVIEW", PreviewTable("").bindProperty("schema"))
        )

    def validate(self, context: WorkflowContext, component: Component) -> list:
        diagnostics = super(bigquery, self).validate(context, component)
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        return newState

    class BigQueryFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: bigquery.BigQueryProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            reader = spark.read.format("bigquery")
            if self.props.credentialsFile is not None:
                import base64

                if self.props.credType == "credentialsFile":
                    rdd = spark.sparkContext.wholeTextFiles(self.props.credentialsFile)
                    credentials = base64.b64encode(
                        bytes(rdd.first()[1], "utf-8")
                    ).decode("utf-8")
                    reader = reader.option("credentials", credentials)
                if self.props.credType == "databricksSecrets":
                    credentials = (
                        self.props.secretsVariable
                        if self.props.isBase64
                        else base64.b64encode(
                            bytes(self.props.secretsVariable, "utf-8")
                        ).decode("utf-8")
                    )
                    reader = reader.option("credentials", credentials)
            if self.props.datasetOption is not None:
                reader = reader.option("dataset", self.props.datasetOption)
            if self.props.project is not None:
                reader = reader.option("project", self.props.project)
            if self.props.maxParallelism is not None:
                reader = reader.option("maxParallelism", int(self.props.maxParallelism))
            if self.props.preferredMinParallelism is not None:
                reader = reader.option(
                    "preferredMinParallelism", int(self.props.preferredMinParallelism)
                )
            if self.props.viewsEnabled is not None:
                reader = reader.option("viewsEnabled", self.props.viewsEnabled)
            if self.props.materializationProject is not None:
                reader = reader.option(
                    "materializationProject", self.props.materializationProject
                )
            if self.props.materializationDataset is not None:
                reader = reader.option(
                    "materializationDataset", self.props.materializationDataset
                )
            if self.props.materializationExpirationTimeInMinutes is not None:
                reader = reader.option(
                    "materializationExpirationTimeInMinutes",
                    int(self.props.materializationExpirationTimeInMinutes),
                )
            if self.props.readDataFormat is not None:
                reader = reader.option("readDataFormat", self.props.readDataFormat)
            if self.props.optimizedEmptyProjection is not None:
                reader = reader.option(
                    "optimizedEmptyProjection", self.props.optimizedEmptyProjection
                )
            if self.props.pushAllFilters is not None:
                reader = reader.option("pushAllFilters", self.props.pushAllFilters)
            if self.props.bigQueryJobLabel is not None:
                reader = reader.option("bigQueryJobLabel", self.props.bigQueryJobLabel)
            if self.props.traceApplicationName is not None:
                reader = reader.option(
                    "traceApplicationName", self.props.traceApplicationName
                )
            if self.props.traceJobId is not None:
                reader = reader.option("traceJobId", self.props.traceJobId)
            if self.props.proxyAddress is not None:
                reader = reader.option("proxyAddress", self.props.proxyAddress)
            if self.props.proxyUsername is not None:
                reader = reader.option("proxyUsername", self.props.proxyUsername)
            if self.props.proxyPassword is not None:
                reader = reader.option("proxyPassword", self.props.proxyPassword)
            if self.props.httpMaxRetry is not None:
                reader = reader.option("httpMaxRetry", int(self.props.httpMaxRetry))
            if self.props.httpConnectTimeout is not None:
                reader = reader.option(
                    "httpConnectTimeout", int(self.props.httpConnectTimeout)
                )
            if self.props.httpReadTimeout is not None:
                reader = reader.option(
                    "httpReadTimeout", int(self.props.httpReadTimeout)
                )
            if self.props.arrowCompressionCodec is not None:
                reader = reader.option(
                    "arrowCompressionCodec", self.props.arrowCompressionCodec
                )
            if self.props.cacheExpirationTimeInMinutes is not None:
                reader = reader.option(
                    "cacheExpirationTimeInMinutes",
                    int(self.props.cacheExpirationTimeInMinutes),
                )
            if self.props.createReadSessionTimeoutInSeconds is not None:
                reader = reader.option(
                    "createReadSessionTimeoutInSeconds",
                    int(self.props.createReadSessionTimeoutInSeconds),
                )
            if self.props.datetimeZoneId is not None:
                reader = reader.option("datetimeZoneId", self.props.datetimeZoneId)
            if self.props.queryJobPriority is not None:
                reader = reader.option("queryJobPriority", self.props.queryJobPriority)
            if self.props.gcpAccessToken is not None:
                reader = reader.option("gcpAccessToken", self.props.gcpAccessToken)

            return (
                reader.option("table", self.props.table)
                    .option("parentProject", self.props.parentProject)
                    .load()
            )

        def targetApply(self, spark: SparkSession, in0: DataFrame):
            writer = in0.write.format("bigquery")
            if self.props.credentialsFile is not None:
                import base64

                if self.props.credType == "credentialsFile":
                    rdd = spark.sparkContext.wholeTextFiles(self.props.credentialsFile)
                    credentials = base64.b64encode(
                        bytes(rdd.first()[1], "utf-8")
                    ).decode("utf-8")
                    writer = writer.option("credentials", credentials)
                if self.props.credType == "databricksSecrets":
                    credentials = (
                        self.props.secretsVariable
                        if self.props.isBase64
                        else base64.b64encode(
                            bytes(self.props.secretsVariable, "utf-8")
                        ).decode("utf-8")
                    )
                    writer = writer.option("credentials", credentials)
            if self.props.datasetOption is not None:
                writer = writer.option("dataset", self.props.datasetOption)
            if self.props.project is not None:
                writer = writer.option("project", self.props.project)
            if self.props.bigQueryTableLabel is not None:
                writer = writer.option(
                    "bigQueryTableLabel", self.props.bigQueryTableLabel
                )
            if self.props.createDisposition is not None:
                writer = writer.option(
                    "createDisposition", self.props.createDisposition
                )
            if self.props.writeMethod is not None:
                writer = writer.option("writeMethod", self.props.writeMethod)
            if self.props.temporaryGcsBucket is not None:
                writer = writer.option(
                    "temporaryGcsBucket", self.props.temporaryGcsBucket
                )
            if self.props.persistentGcsBucket is not None:
                writer = writer.option(
                    "persistentGcsBucket", self.props.persistentGcsBucket
                )
            if self.props.persistentGcsPath is not None:
                writer = writer.option(
                    "persistentGcsPath", self.props.persistentGcsPath
                )
            if self.props.intermediateFormat is not None:
                writer = writer.option(
                    "intermediateFormat", self.props.intermediateFormat
                )
            if self.props.useAvroLogicalTypes is not None:
                writer = writer.option(
                    "useAvroLogicalTypes", self.props.useAvroLogicalTypes
                )
            if self.props.datePartition is not None:
                writer = writer.option("datePartition", self.props.datePartition)
            if self.props.partitionField is not None:
                writer = writer.option("partitionField", self.props.partitionField)
            if self.props.partitionExpirationMs is not None:
                writer = writer.option(
                    "partitionExpirationMs", int(self.props.partitionExpirationMs)
                )
            if self.props.partitionType is not None:
                writer = writer.option("partitionType", self.props.partitionType)
            if (
                    self.props.clusteredFields is not None
                    and len(self.props.clusteredFields) > 0
            ):
                writer = writer.option(
                    "clusteredFields", ",".join(self.props.clusteredFields)
                )
            if self.props.allowFieldAddition is not None:
                writer = writer.option(
                    "allowFieldAddition", self.props.allowFieldAddition
                )
            if self.props.allowFieldRelaxation is not None:
                writer = writer.option(
                    "allowFieldRelaxation", self.props.allowFieldRelaxation
                )
            if self.props.proxyUsername is not None:
                writer = writer.option("proxyUsername", self.props.proxyUsername)
            if self.props.proxyPassword is not None:
                writer = writer.option("proxyPassword", self.props.proxyPassword)
            if self.props.httpMaxRetry is not None:
                writer = writer.option("httpMaxRetry", int(self.props.httpMaxRetry))
            if self.props.httpConnectTimeout is not None:
                writer = writer.option(
                    "httpConnectTimeout", int(self.props.httpConnectTimeout)
                )
            if self.props.enableModeCheckForSchemaFields is not None:
                writer = writer.option(
                    "enableModeCheckForSchemaFields",
                    self.props.enableModeCheckForSchemaFields,
                )
            if self.props.enableListInference is not None:
                writer = writer.option(
                    "enableListInference", self.props.enableListInference
                )
            if self.props.datetimeZoneId is not None:
                writer = writer.option("datetimeZoneId", self.props.datetimeZoneId)
            if self.props.queryJobPriority is not None:
                writer = writer.option("queryJobPriority", self.props.queryJobPriority)
            if self.props.gcpAccessToken is not None:
                writer = writer.option("gcpAccessToken", self.props.gcpAccessToken)

            writer = writer.option("parentProject", self.props.parentProject)

            if self.props.writeMode is not None:
                writer = writer.mode(self.props.writeMode)

            writer.save(self.props.table)
