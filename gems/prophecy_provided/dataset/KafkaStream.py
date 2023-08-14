from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode, Diagnostic, SeverityLevelEnum
from prophecy.cb.server.base.DatasetBuilderBase import (
    DatasetSpec,
    DatasetProperties,
    Component,
)
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.StringUtils import isBlank
from prophecy.cb.server.base import WorkflowContext

class KafkaStream(DatasetSpec):
    name: str = "KafkaStream"
    datasetType: str = "File"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/source-target/file/kafka"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class KafkaStreamProperties(DatasetProperties):
        schema: Optional[StructType] = None
        description: Optional[str] = ""
        credType: str = "userPwd"
        credentialScope: Optional[str] = None
        brokerList: Optional[str] = None
        groupId: Optional[str] = ""
        sessionTimeout: Optional[str] = "6000"
        security: Optional[str] = "SASL_SSL"
        saslMechanism: Optional[str] = "SCRAM-SHA-256"
        textUsername: Optional[str] = None
        textPassword: Optional[str] = None
        kafkaTopic: Optional[str] = None
        offsetMetaTable: Optional[str] = ""
        messageKey: Optional[SColumn] = None

    def sourceDialog(self) -> DatasetDialog:
        return (
            DatasetDialog("KafkaStream")
                .addSection(
                "LOCATION",
                StackLayout()
                    .addElement(
                    TextBox("Broker List")
                        .bindPlaceholder("broker1.aws.com:9094,broker2.aws.com:9094")
                        .bindProperty("brokerList")
                )
                    .addElement(TextBox("Group Id").bindPlaceholder("group_id_1").bindProperty("groupId"))
                    .addElement(
                    TextBox("Session timeout (in ms)")
                        .bindPlaceholder("6000")
                        .bindProperty("sessionTimeout")
                )
                    .addElement(
                    SelectBox("Security Protocol")
                        .addOption("SASL_SSL", "securitySASL")
                        .bindProperty("security")
                )
                    .addElement(
                    StackLayout()
                        .addElement(
                        SelectBox("SASL Mechanisms")
                            .addOption("SCRAM-SHA-256", "sha256Mechanism")
                            .bindProperty("saslMechanism")
                    ).addElement(
                        StackLayout()
                            .addElement(
                            RadioGroup("Credentials")
                                .addOption("Databricks Secrets", "databricksSecrets")
                                .addOption("Username & Password", "userPwd")
                                .bindProperty("credType")
                        ))
                        .addElement(
                        Condition()
                            .ifEqual(PropExpr("component.properties.credType"), StringExpr("databricksSecrets"))
                            .then(Credentials("").bindProperty("credentialScope"))
                            .otherwise(
                            ColumnsLayout(gap=("1rem"))
                                .addColumn(TextBox("Username").bindPlaceholder("username").bindProperty("textUsername"))
                                .addColumn(
                                TextBox("Password").isPassword().bindPlaceholder("password").bindProperty(
                                    "textPassword")
                            )
                        )
                    )
                )
                    .addElement(
                    ColumnsLayout(gap=("1rem"))
                        .addColumn(
                        TextBox("Kafka topic")
                            .bindPlaceholder("my_first_topic,my_second_topic")
                            .bindProperty("kafkaTopic")
                    )
                        .addColumn(
                        TextBox("Metadata Table")
                            .bindPlaceholder("db.metaTable")
                            .bindProperty("offsetMetaTable")
                    )
                ),
            )
                .addSection(
                "PROPERTIES",
                ColumnsLayout(gap=("1rem"), height=("100%")).addColumn(
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
                            )
                        )
                    ),
                    "auto"
                ).addColumn(
                    SchemaTable("").isReadOnly().bindProperty("schema"), "5fr"
                ),
            )
                .addSection("PREVIEW", PreviewTable("").bindProperty("schema"))
        )

    def targetDialog(self) -> DatasetDialog:
        return DatasetDialog("KafkaStream").addSection(
            "PROPERTIES",
            StackLayout()
                .addElement(
                TextBox("Broker List")
                    .bindPlaceholder("broker1.aws.com:9094,broker2.aws.com:9094")
                    .bindProperty("brokerList")
            )
                .addElement(
                SelectBox("Security Protocol")
                    .addOption("SASL_SSL", "securitySASL")
                    .bindProperty("security")
            )
                .addElement(
                StackLayout()
                    .addElement(
                    SelectBox("SASL Mechanisms")
                        .addOption("SCRAM-SHA-256", "sha256Mechanism")
                        .bindProperty("saslMechanism")
                ).addElement(
                    StackLayout()
                        .addElement(
                        RadioGroup("Credentials")
                            .addOption("Databricks Secrets", "databricksSecrets")
                            .addOption("Username & Password", "userPwd")
                            .bindProperty("credType")
                    ))
                    .addElement(
                    Condition()
                        .ifEqual(PropExpr("component.properties.credType"), StringExpr("databricksSecrets"))
                        .then(Credentials("").bindProperty("credentialScope"))
                        .otherwise(
                        ColumnsLayout(gap=("1rem"))
                            .addColumn(TextBox("Username").bindPlaceholder("username").bindProperty("textUsername"))
                            .addColumn(
                            TextBox("Password").isPassword().bindPlaceholder("password").bindProperty(
                                "textPassword")
                        )
                    )
                )
            )
                .addElement(
                ColumnsLayout(gap=("1rem"))
                    .addColumn(
                    TextBox("Kafka topic")
                        .bindPlaceholder("my_first_topic,my_second_topic")
                        .bindProperty("kafkaTopic")
                )
            ).addElement(
                StackLayout()
                    .addElement(
                    NativeText(
                        "Message Unique Key (Optional)"
                    )
                )
                    .addElement(
                    ExpressionBox(ignoreTitle=True)
                        .makeFieldOptional()
                        .withSchemaSuggestions()
                        .bindPlaceholders({
                        "scala": """concat(col("col1"), col("col2"))""",
                        "python": """concat(col("col1"), col("col2"))""",
                        "sql": """concat(col1, col2)"""})
                        .bindProperty("messageKey")
                )
            )
        )

    def validate(self, context: WorkflowContext, component: Component) -> list:
        diagnostics = super(KafkaStream, self).validate(context, component)

        if component.properties.brokerList is None or isBlank(component.properties.brokerList):
            diagnostics.append(
                Diagnostic("properties.brokerList", "Please provide at least 1 broker.", SeverityLevelEnum.Error))

        # if component.properties.groupId is None or isBlank(component.properties.groupId):
        #     diagnostics.append(
        #         Diagnostic("properties.groupId", "Consumer group id cannot be blank", SeverityLevelEnum.Error))

        if component.properties.security is None or isBlank(component.properties.security):
            diagnostics.append(
                Diagnostic("properties.security", "Please choose at least one security protocol",
                           SeverityLevelEnum.Error))

        if component.properties.kafkaTopic is None or isBlank(component.properties.kafkaTopic):
            diagnostics.append(
                Diagnostic("properties.kafkaTopic", "Please provide at least one kafka topic",
                           SeverityLevelEnum.Error))

        if component.properties.credType == "databricksSecrets":
            if isBlank(component.properties.credentialScope):
                diagnostics.append(Diagnostic(
                    "properties.credentialScope",
                    "Credential Scope cannot be empty",
                    SeverityLevelEnum.Error))
        elif component.properties.credType == "userPwd":
            if isBlank(component.properties.textUsername):
                diagnostics.append(Diagnostic("properties.textUsername", "Username cannot be empty",
                                              SeverityLevelEnum.Error))
            elif isBlank(component.properties.textPassword):
                diagnostics.append(Diagnostic("properties.textPassword", "Password cannot be empty [Location]",
                                              SeverityLevelEnum.Error))

        # if component.properties.offsetMetaTable is None or isBlank(component.properties.offsetMetaTable):
        #     diagnostics.append(
        #         Diagnostic("properties.offsetMetaTable", "Metadata table name to store offsets cannot be blank.", SeverityLevelEnum.Error))

        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        return newState

    class KafkaStreamFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: KafkaStream.KafkaStreamProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            if not ("SColumnExpression" in locals()):
                from delta.tables import DeltaTable
                import json

                tableExists = spark.catalog._jcatalog.tableExists(
                    f"{self.props.offsetMetaTable}"
                )

                if self.props.credType == "databricksSecrets":
                    from pyspark.dbutils import DBUtils
                    dbutils = DBUtils(spark)
                    username = dbutils.secrets.get(scope=self.props.credentialScope, key="username")
                    password = dbutils.secrets.get(scope=self.props.credentialScope, key="password")
                else:
                    username = self.props.textUsername
                    password = self.props.textPassword

                if tableExists:
                    deltaTable = DeltaTable.forName(
                        spark, f"{self.props.offsetMetaTable}"
                    ).toDF()

                    meta = deltaTable.collect()

                    offset_dict = {}
                    for row in meta:
                        if row["topic"] in offset_dict.keys():
                            offset_dict[row["topic"]].update(
                                {row["partition"]: row["max_offset"] + 1}
                            )
                        else:
                            offset_dict[row["topic"]] = {
                                row["partition"]: row["max_offset"] + 1
                            }

                    consumer_options = {
                        "kafka.sasl.jaas.config": f"kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule"
                                                  + f' required username="{username}" password="{password}";',
                        "kafka.sasl.mechanism": self.props.saslMechanism,
                        "kafka.security.protocol": self.props.security,
                        "kafka.bootstrap.servers": self.props.brokerList,
                        "kafka.session.timeout.ms": self.props.sessionTimeout,
                        "group.id": self.props.groupId,
                        "subscribe": self.props.kafkaTopic,
                        "startingOffsets": json.dumps(offset_dict),
                    }

                    df = (
                        spark.read.format("kafka")
                            .options(**consumer_options)
                            .load()
                            .withColumn("value", col("value").cast("string"))
                            .withColumn("key", col("key").cast("string"))
                    )
                    return df
                else:
                    consumer_options = {
                        "kafka.sasl.jaas.config": f"kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule"
                                                  + f' required username="{username}" password="{password}";',
                        "kafka.sasl.mechanism": self.props.saslMechanism,
                        "kafka.security.protocol": self.props.security,
                        "kafka.bootstrap.servers": self.props.brokerList,
                        "kafka.session.timeout.ms": self.props.sessionTimeout,
                        "group.id": self.props.groupId,
                        "subscribe": self.props.kafkaTopic,
                    }
                    df = (
                        spark.read.format("kafka")
                            .options(**consumer_options)
                            .load()
                            .withColumn("value", col("value").cast("string"))
                            .withColumn("key", col("key").cast("string"))
                    )
                    return df
            else:
                return spark.createDataFrame([[1, 1], [2, 2]])

        def targetApply(self, spark: SparkSession, in0: DataFrame):
            if self.props.credType == "databricksSecrets":
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(spark)
                username = dbutils.secrets.get(scope=self.props.credentialScope, key="username")
                password = dbutils.secrets.get(scope=self.props.credentialScope, key="password")
            else:
                username = self.props.textUsername
                password = self.props.textPassword

            producer_options = {
                "kafka.sasl.jaas.config": f"kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule"
                                          + f' required username="{username}" password="{password}";',
                "kafka.sasl.mechanism": self.props.saslMechanism,
                "kafka.security.protocol": self.props.security,
                "kafka.bootstrap.servers": self.props.brokerList,
                "topic": self.props.kafkaTopic,
            }
            if self.props.messageKey is None:
                in0.select(to_json(struct("*")).alias("value")).selectExpr(
                    "CAST(value AS STRING)"
                ).write.format("kafka").options(**producer_options).save()
            else:
                in0.select(self.props.messageKey.column().alias("key"), to_json(struct("*")).alias("value")).selectExpr(
                    "CAST(key AS STRING)", "CAST(value AS STRING)"
                ).write.format("kafka").options(**producer_options).save()
