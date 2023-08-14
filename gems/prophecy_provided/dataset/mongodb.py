from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode, Diagnostic, SeverityLevelEnum
from prophecy.cb.server.base.DatasetBuilderBase import DatasetSpec, DatasetProperties, Component
from prophecy.cb.ui.uispec import *

# from prophecy.cb.ui.UISpecUtil import DBUtils
from prophecy.cb.util.StringUtils import isBlank
from prophecy.cb.server.base import WorkflowContext

class mongodb(DatasetSpec):
    name: str = "mongodb"
    datasetType: str = "Warehouse"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/source-target/warehouse/mongodb"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class MongoDBProperties(DatasetProperties):
        schema: Optional[StructType] = None
        description: Optional[str] = ""
        credentialScope: Optional[str] = None
        writeMode: Optional[str] = "overwrite"
        textUsername: Optional[str] = None
        textPassword: Optional[str] = None
        database: Optional[str] = None
        collection: Optional[str] = None
        mongoClientFactory: Optional[str] = None
        driver: Optional[str] = "mongodb"
        clusterIpAddress: Optional[str] = None
        partitioner: Optional[str] = None
        partitionerOptionsPartitionField: Optional[str] = None
        partitionerOptionsPartitionSize: Optional[str] = None
        partitionerOptionsSamplesPerPartition: Optional[str] = None
        sampleSize: Optional[str] = None
        sqlInferSchemaMapTypesEnabled: Optional[bool] = None
        sqlInferSchemaMapTypesMinimumKeySize: Optional[str] = None
        aggregationPipeline: Optional[str] = None
        aggregationAllowDiskUse: Optional[bool] = None
        maxBatchSize: Optional[str] = None
        ordered: Optional[bool] = None
        operationType: Optional[str] = None
        idFieldList: Optional[str] = None
        writeConcernW: Optional[str] = None
        writeConcernJournal: Optional[bool] = None
        writeConcernWTimeoutMs: Optional[str] = None

    def sourceDialog(self) -> DatasetDialog:

        urlSection = StackLayout() \
            .addElement(TitleElement(title="Build Connection URI")) \
            .addElement(
            ColumnsLayout(gap=("1rem"))
            .addColumn(
                SelectBox("Driver")
                .addOption("mongodb", "mongodb")
                .addOption("mongodb+srv", "mongodb+srv")
                .bindProperty("driver"))
            .addColumn(
                TextBox("Cluster IP Address and Options")
                .bindPlaceholder("cluster0.prophecy.mongodb.net/?retryWrites=true&w=majority")
                .bindProperty("clusterIpAddress")
            )
        ) \
            .addElement(TitleElement(title="Source")) \
            .addElement(
            ColumnsLayout(gap=("1rem"))
            .addColumn(TextBox("Database").bindPlaceholder("database").bindProperty("database"))
            .addColumn(TextBox("Collection").bindPlaceholder("collection").bindProperty("collection"))
        )

        return DatasetDialog("mongodb") \
            .addSection(
            "LOCATION",
            ColumnsLayout()
            .addColumn(
                StackLayout(direction=("vertical"), gap=("1rem"))
                .addElement(
                    StackLayout()
                    .addElement(
                        TitleElement(title="Credentials")
                    )
                    .addElement(
                        ColumnsLayout(gap=("1rem"))
                        .addColumn(TextBox("Username").bindPlaceholder("username").bindProperty("textUsername"))
                        .addColumn(
                            TextBox("Password").isPassword().bindPlaceholder("password").bindProperty(
                                "textPassword")
                        )
                    )
                )
                .addElement(urlSection)
            )
        ) \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap=("1rem"), height=("100%"))
            .addColumn(
                ScrollBox()
                .addElement(
                    StackLayout()
                    .addElement(
                        StackItem(grow=1).addElement(
                            FieldPicker(height=("100%"))
                            .addField(
                                TextArea("Description", 2, placeholder="Dataset description..."),
                                "description",
                                True
                            )
                            .addField(
                                TextBox(
                                    "Mongo client factory",
                                    placeholder="com.mongodb.spark.sql.connector.connection.DefaultMongoClientFactory"
                                ),
                                "mongoClientFactory"
                            )
                            .addField(
                                TextBox(
                                    "partitioner class name",
                                    placeholder="com.mongodb.spark.sql.connector.read.partitioner.SamplePartitioner"
                                ),
                                "partitioner"
                            )
                            .addField(
                                TextBox("Partition field", placeholder="_id"),
                                "partitionerOptionsPartitionField"
                            )
                            .addField(
                                TextBox("Partition size", placeholder="64"),
                                "partitionerOptionsPartitionSize"
                            )
                            .addField(
                                TextBox("Number of samples per partition", placeholder="10"),
                                "partitionerOptionsSamplesPerPartition"
                            )
                            .addField(TextBox("Minimum no. of Docs for Schema inference", placeholder="1000"),
                                      "sampleSize")
                            .addField(
                                Checkbox("Enable Map types when inferring schema"), "sqlInferSchemaMapTypesEnabled"
                            )
                            .addField(
                                TextBox("Minimum no. of a StructType for MapType inference", placeholder="250"),
                                "sqlInferSchemaMapTypesMinimumKeySize"
                            )
                            .addField(
                                TextBox("Pipeline aggregation", placeholder=""""{"$match": {"closed": false}}"""),
                                "aggregationPipeline"
                            )
                            .addField(
                                Checkbox("Enable AllowDiskUse aggregation"), "aggregationAllowDiskUse"
                            )
                        )
                    )
                ),
                "auto"
            )
            .addColumn(
                SchemaTable("").bindProperty("schema"), "5fr"
            )
        ) \
            .addSection(
            "PREVIEW",
            PreviewTable("").bindProperty("schema")
        )

    def targetDialog(self) -> DatasetDialog:
        urlSection = StackLayout() \
            .addElement(TitleElement(title="Build Connection URI")) \
            .addElement(
            ColumnsLayout(gap=("1rem"))
            .addColumn(
                SelectBox("Driver")
                .addOption("mongodb", "mongodb")
                .addOption("mongodb+srv", "mongodb+srv")
                .bindProperty("driver"))
            .addColumn(
                TextBox("Cluster IP Address and Options")
                .bindPlaceholder("cluster0.prophecy.mongodb.net/?retryWrites=true&w=majority")
                .bindProperty("clusterIpAddress")
            )
        ) \
            .addElement(TitleElement(title="Source")) \
            .addElement(
            ColumnsLayout(gap=("1rem"))
            .addColumn(TextBox("Database").bindPlaceholder("database").bindProperty("database"))
            .addColumn(TextBox("Collection").bindPlaceholder("collection").bindProperty("collection"))
        )

        return DatasetDialog("mongodb") \
            .addSection(
            "LOCATION",
            ColumnsLayout()
            .addColumn(
                StackLayout(direction=("vertical"), gap=("1rem"))
                .addElement(
                    StackLayout()
                    .addElement(
                        TitleElement(title="Credentials")
                    )
                    .addElement(
                        ColumnsLayout(gap=("1rem"))
                        .addColumn(TextBox("Username").bindPlaceholder("username").bindProperty("textUsername"))
                        .addColumn(
                            TextBox("Password").isPassword().bindPlaceholder("password").bindProperty(
                                "textPassword")
                        )
                    )
                )
                .addElement(urlSection)
            )) \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap=("1rem"), height=("100%"))
            .addColumn(
                ScrollBox().addElement(
                    StackLayout(height=("100%")).addElement(
                        StackItem(grow=(1)).addElement(
                            FieldPicker(height=("100%"))
                            .addField(
                                TextArea("Description", 2, placeholder="Dataset description..."),
                                "description",
                                True
                            )
                            .addField(
                                SelectBox("Write Mode")
                                .addOption("overwrite", "overwrite")
                                .addOption("append", "append"),
                                "writeMode"
                            )
                            .addField(
                                TextBox(
                                    "Mongo client factory",
                                    placeholder="com.mongodb.spark.sql.connector.connection.DefaultMongoClientFactory"
                                ),
                                "mongoClientFactory"
                            )
                            .addField(TextBox("Maximum batch size", placeholder="512"), "maxBatchSize")
                            .addField(Checkbox("ordered"), "ordered")
                            .addField(
                                SelectBox("operationType")
                                .addOption("insert", "insert")
                                .addOption("replace", "replace")
                                .addOption("update", "update"),
                                "operationType"
                            )
                            .addField(TextBox("List of id fields", placeholder="_id"), "idFieldList")
                            .addField(
                                SelectBox("writeConcern.w")
                                .addOption("MAJORITY", "MAJORITY")
                                .addOption("W1", "W1")
                                .addOption("W2", "W2")
                                .addOption("W3", "W3")
                                .addOption("ACKNOWLEDGED", "ACKNOWLEDGED")
                                .addOption("UNACKNOWLEDGED", "UNACKNOWLEDGED"),
                                "writeConcernW"
                            )
                            .addField(Checkbox("Enable Write journal"), "writeConcernJournal")
                            .addField(TextBox("Write timeout in MSec", placeholder="0"), "writeConcernWTimeoutMs")
                        )
                    )
                ),
                "auto"
            )
            .addColumn(
                StackLayout()
                .addElement(
                    StackLayout(height=("80bh"))
                    .addElement(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"))
                ),
                "5fr"
            )
        )

    def validate(self, context: WorkflowContext, component: Component) -> list:
        diagnostics = super(mongodb, self).validate(context, component)
        if isBlank(component.properties.textUsername):
            diagnostics.append(
                Diagnostic(
                    "properties.textUsername",
                    "Username cannot be empty [Location]",
                    SeverityLevelEnum.Error))
        if isBlank(component.properties.textPassword):
            diagnostics.append(
                Diagnostic(
                    "properties.textPassword",
                    "Password cannot be empty [Location]",
                    SeverityLevelEnum.Error))
        if isBlank(component.properties.driver):
            diagnostics.append(
                Diagnostic(
                    "properties.driver",
                    "Driver cannot be empty, example values [mongodb, mongodb+srv]",
                    SeverityLevelEnum.Error))

        if isBlank(component.properties.clusterIpAddress):
            diagnostics.append(
                Diagnostic(
                    "properties.clusterIpAddress",
                    "Cluster IP Address and Options cannot be empty, eg. 'cluster0.prophecy.mongodb.net/?retryWrites=true&w=majority'",
                    SeverityLevelEnum.Error))

        if isBlank(component.properties.database):
            diagnostics.append(Diagnostic(
                "properties.database",
                "Database name cannot be empty",
                SeverityLevelEnum.Error
            ))

        if isBlank(component.properties.collection):
            diagnostics.append(Diagnostic(
                "properties.collection",
                "Collection name cannot be empty",
                SeverityLevelEnum.Error
            ))
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        return newState

    class MongoDBFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: mongodb.MongoDBProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            conn_uri = f'{self.props.driver}://{self.props.textUsername}:{self.props.textPassword}@{self.props.clusterIpAddress}'

            reader = spark.read.format("mongodb") \
                .option("connection.uri", conn_uri) \
                .option("database", self.props.database) \
                .option("collection", self.props.collection)

            if self.props.mongoClientFactory:
                reader = reader.option(
                    "mongoClientFactory",
                    self.props.mongoClientFactory)
            if self.props.partitioner:
                reader = reader.option("partitioner", self.props.partitioner)
            if self.props.partitionerOptionsPartitionField:
                reader = reader.option(
                    "partitioner.options.partition.field",
                    self.props.partitionerOptionsPartitionField)
            if self.props.partitionerOptionsPartitionSize:
                reader = reader.option(
                    "partitioner.options.partition.size",
                    self.props.partitionerOptionsPartitionSize)
            if self.props.partitionerOptionsSamplesPerPartition:
                reader = reader.option("partitioner.options.samples.per.partition",
                              self.props.partitionerOptionsSamplesPerPartition)
            if self.props.sampleSize:
                reader = reader.option("sampleSize", self.props.sampleSize)
            if self.props.sqlInferSchemaMapTypesEnabled:
                reader = reader.option(
                    "sql.inferSchema.mapTypes.enabled",
                    self.props.sqlInferSchemaMapTypesEnabled)
            if self.props.sqlInferSchemaMapTypesMinimumKeySize:
                reader = reader.option("sql.inferSchema.mapTypes.minimum.key.size",
                              self.props.sqlInferSchemaMapTypesMinimumKeySize)
            if self.props.aggregationPipeline:
                reader = reader.option(
                    "aggregation.pipeline",
                    self.props.aggregationPipeline)
            if self.props.aggregationAllowDiskUse:
                reader = reader.option(
                    "aggregation.allowDiskUse",
                    self.props.aggregationAllowDiskUse)

            return reader.load()

        def targetApply(self, spark: SparkSession, in0: DataFrame):
            conn_uri = f'{self.props.driver}://{self.props.textUsername}:{self.props.textPassword}@{self.props.clusterIpAddress}'

            writer = in0.write.format("mongodb") \
                .option("connection.uri", conn_uri) \
                .option("database", self.props.database) \
                .option("collection", self.props.collection)

            if self.props.mongoClientFactory:
                writer = writer.option(
                    "mongoClientFactory",
                    self.props.mongoClientFactory)
            if self.props.maxBatchSize:
                writer = writer.option("maxBatchSize", self.props.maxBatchSize)
            if self.props.ordered:
                writer = writer.option("ordered", self.props.ordered)
            if self.props.operationType:
                writer = writer.option("operationType", self.props.operationType)
            if self.props.idFieldList:
                writer = writer.option("idFieldList", self.props.idFieldList)
            if self.props.writeConcernW:
                writer = writer.option("writeConcern.w", self.props.writeConcernW)
            if self.props.writeConcernJournal:
                writer = writer.option(
                    "writeConcern.journal",
                    self.props.writeConcernJournal)
            if self.props.writeConcernWTimeoutMs:
                writer = writer.option(
                    "writeConcern.wTimeoutMS",
                    self.props.writeConcernWTimeoutMs)

            writer.save()
