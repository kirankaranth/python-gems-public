from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode, Diagnostic, SeverityLevelEnum, \
    SubstituteDisabled
from prophecy.cb.server.base.DatasetBuilderBase import DatasetSpec, DatasetProperties, Component
from prophecy.cb.ui.uispec import *

# from prophecy.cb.ui.UISpecUtil import DBUtils
from prophecy.cb.util.StringUtils import isBlank
from prophecy.cb.server.base import WorkflowContext

class snowflake(DatasetSpec):
    name: str = "snowflake"
    datasetType: str = "Warehouse"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/source-target/warehouse/snowflake"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class SnowflakeProperties(DatasetProperties):
        schema: Optional[StructType] = None
        description: Optional[str] = ""
        credType: str = "databricksSecrets"
        credentialScope: Optional[str] = None
        textUsername: Optional[str] = None
        textPassword: Optional[str] = None
        keyFilepath: Optional[str] = None
        keyPasskey: Optional[str] = None
        privateKeyFormat: Optional[str] = "PKCS8"
        url: str = ""
        database: str = ""
        schemaName: str = ""
        warehouse: str = ""
        role: Optional[str] = None
        readFromSource: str = "dbtable"
        dbtable: Optional[str] = None
        query: Optional[str] = None
        writeMode: Optional[str] = "overwrite"
        postSql: Optional[str] = None
        shouldPostSql: Optional[bool] = None

    def sourceDialog(self) -> DatasetDialog:
        urlSection = StackLayout() \
            .addElement(TitleElement(title="URL")) \
            .addElement(
            ColumnsLayout(gap="1rem")
            .addColumn(
                TextBox("URL")
                .bindPlaceholder("https://***.us-east-1.snowflakecomputing.com")
                .bindProperty("url")
            )
            .addColumn(TextBox("Database").bindPlaceholder("database").bindProperty("database"))
        ) \
            .addElement(
            ColumnsLayout(gap="1rem")
            .addColumn(TextBox("Schema").bindPlaceholder("schema").bindProperty("schemaName"))
            .addColumn(TextBox("Warehouse").bindPlaceholder("warehouse").bindProperty("warehouse"))
            .addColumn(TextBox("Role").bindPlaceholder("role").bindProperty("role"))
        )
        return DatasetDialog("snowflake") \
            .addSection(
            "LOCATION",
            ColumnsLayout()
            .addColumn(
                StackLayout(direction="vertical", gap="1rem")
                #            .addElement(TitleElement(title = "Credentials"))
                .addElement(
                    StackLayout()
                    .addElement(
                        RadioGroup("Credentials")
                        .addOption("Databricks Secrets", "databricksSecrets")
                        .addOption("Username & Password", "userPwd")
                        .addOption("Key Pair Authentication", "userP8")
                        .bindProperty("credType")
                    )
                    .addElement(
                        Condition()
                        .ifEqual(PropExpr("component.properties.credType"), StringExpr("databricksSecrets"))
                        .then(Credentials("").bindProperty("credentialScope"))
                        .otherwise(
                            Condition()
                            .ifEqual(PropExpr("component.properties.credType"), StringExpr("userP8"))
                            .then(
                                ColumnsLayout(gap="1rem")
                                .addColumn(TextBox("Username").bindPlaceholder("username").bindProperty("textUsername"))
                                .addColumn(TextBox("Private key path in PKCS8 format")
                                           .bindPlaceholder("${config_private_key_path}")
                                           .bindProperty("keyFilepath")
                                           )
                                .addElement(TextBox("Private key passphrase").isPassword()
                                            .bindPlaceholder("${config_passphrase}")
                                            .bindProperty("keyPasskey"))
                            )
                            .otherwise(
                                StackLayout()
                                .addElement(
                                    ColumnsLayout(gap="1rem")
                                    .addColumn(
                                        TextBox("Username").bindPlaceholder("username").bindProperty("textUsername"))
                                    .addColumn(
                                        TextBox("Password").isPassword().bindPlaceholder("password").bindProperty(
                                            "textPassword")
                                    )
                                )
                                .addElement(
                                    ColumnsLayout()
                                    .addColumn(
                                        AlertBox(
                                            variant="warning",
                                            _children=[
                                                Markdown(
                                                    "Storing plain-text passwords poses a security risk and is not "
                                                    "recommended. Please see [here]("
                                                    "https://docs.prophecy.io/low-code-spark/best-practices/use-dbx-secrets) for suggested alternatives"
                                                )
                                            ]
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
                .addElement(urlSection)
                .addElement(
                    StackLayout()
                    .addElement(
                        RadioGroup("Data Source")
                        .addOption("DB Table", "dbtable")
                        .addOption("SQL Query", "query")
                        .bindProperty("readFromSource")
                    )
                    .addElement(
                        Condition()
                        .ifEqual(PropExpr("component.properties.readFromSource"), StringExpr("dbtable"))
                        .then(TextBox("Table").bindPlaceholder("tablename").bindProperty("dbtable"))
                        .otherwise(
                            TextBox("SQL Query").bindPlaceholder("select c1, c2 from t1").bindProperty("query")
                        )
                    )
                )
            )
        ) \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap="1rem", height="100%").addColumn(
                ScrollBox().addElement(
                    StackLayout(height="100%")
                    .addElement(
                        StackItem(grow=1).addElement(
                            FieldPicker(height="100%")
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
                SchemaTable("").bindProperty("schema"), "5fr"
            )
        ) \
            .addSection(
            "PREVIEW",
            PreviewTable("").bindProperty("schema")
        )

    def targetDialog(self) -> DatasetDialog:
        urlSection = StackLayout() \
            .addElement(TitleElement(title="URL")) \
            .addElement(
            ColumnsLayout(gap="1rem")
            .addColumn(
                TextBox("URL")
                .bindPlaceholder("https://***.us-east-1.snowflakecomputing.com")
                .bindProperty("url")
            )
            .addColumn(TextBox("Database").bindPlaceholder("database").bindProperty("database"))
        ) \
            .addElement(
            ColumnsLayout(gap="1rem")
            .addColumn(TextBox("Schema").bindPlaceholder("schema").bindProperty("schemaName"))
            .addColumn(TextBox("Warehouse").bindPlaceholder("warehouse").bindProperty("warehouse"))
            .addColumn(TextBox("Role").bindPlaceholder("role").bindProperty("role"))
        )
        return DatasetDialog("snowflake") \
            .addSection(
            "LOCATION",
            ColumnsLayout()
            .addColumn(
                StackLayout(direction="vertical", gap="1rem")
                .addElement(
                    StackLayout()
                    .addElement(
                        RadioGroup("Credentials")
                        .addOption("Databricks Secrets", "databricksSecrets")
                        .addOption("Username & Password", "userPwd")
                        .addOption("Key Pair Authentication", "userP8")
                        .bindProperty("credType")
                    )
                    .addElement(
                        Condition()
                        .ifEqual(PropExpr("component.properties.credType"), StringExpr("databricksSecrets"))
                        .then(Credentials("").bindProperty("credentialScope"))
                        .otherwise(
                            Condition()
                            .ifEqual(PropExpr("component.properties.credType"), StringExpr("userP8"))
                            .then(
                                ColumnsLayout(gap="1rem")
                                .addColumn(TextBox("Username").bindPlaceholder("username").bindProperty("textUsername"))
                                .addColumn(TextBox("Private key path in PKCS8 format")
                                           .bindPlaceholder("${config_private_key_path}")
                                           .bindProperty("keyFilepath")
                                           )
                                .addElement(TextBox("Private key passphrase").isPassword()
                                            .bindPlaceholder("${config_passphrase}")
                                            .bindProperty("keyPasskey"))
                            )
                            .otherwise(
                                StackLayout()
                                .addElement(
                                    ColumnsLayout(gap="1rem")
                                    .addColumn(
                                        TextBox("Username").bindPlaceholder("username").bindProperty("textUsername"))
                                    .addColumn(
                                        TextBox("Password").isPassword().bindPlaceholder("password").bindProperty(
                                            "textPassword")
                                    )
                                )
                                .addElement(
                                    ColumnsLayout()
                                    .addColumn(
                                        AlertBox(
                                            variant="warning",
                                            _children=[
                                                Markdown(
                                                    "Storing plain-text passwords poses a security risk and is not "
                                                    "recommended. Please see [here]("
                                                    "https://docs.prophecy.io/low-code-spark/best-practices/use-dbx-secrets) for suggested alternatives"
                                                )
                                            ]
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
                .addElement(urlSection)
                .addElement(TitleElement(title="Table"))
                .addElement(TextBox("Table").bindPlaceholder("tablename").bindProperty("dbtable"))
            )
        ) \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                ScrollBox().addElement(
                    StackLayout()
                    .addElement(
                        StackItem(grow=1).addElement(
                            FieldPicker(height="100%")
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
                                "writeMode", True
                            )
                        )
                    )
                ),
                "auto"
            )
            .addColumn(
                StackLayout()
                .addElement(
                    StackLayout(height="80bh")
                    .addElement(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"))
                )
                .addElement(TitleElement(title="Query"))
                .addElement(Checkbox("Run post-script SQL").bindProperty("shouldPostSql"))
                .addElement(
                    Condition()
                    .ifEqual(PropExpr("component.properties.shouldPostSql"), BooleanExpr(True))
                    .then(
                        Editor(height="60bh")
                        .withSchemaSuggestions()
                        .bindProperty("postSql")
                    )
                ),
                "5fr"
            )
        )

    def validate(self, context: WorkflowContext, component: Component) -> list:
        diagnostics = super(snowflake, self).validate(context, component)

        if component.properties.credType == "databricksSecrets":
            if isBlank(component.properties.credentialScope):
                diagnostics.append(
                    Diagnostic("properties.credentialScope", "Credential Scope cannot be empty [Location]",
                               SeverityLevelEnum.Error))
        elif component.properties.credType == "userPwd":
            if isBlank(component.properties.textUsername):
                diagnostics.append(Diagnostic("properties.textUsername", "Username cannot be empty [Location]",
                                              SeverityLevelEnum.Error))
            elif isBlank(component.properties.textPassword):
                diagnostics.append(Diagnostic("properties.textPassword", "Password cannot be empty [Location]",
                                              SeverityLevelEnum.Error))
            if isBlank(component.properties.textPassword) or (not component.properties.textPassword.startswith("${")):
                diagnostics.append(Diagnostic("properties.textPassword", "Storing plain-text passwords poses a "
                                                                         "security risk and is not recommended.",
                                              SeverityLevelEnum.Warning))
        elif component.properties.credType == "userP8":
            if isBlank(component.properties.textUsername):
                diagnostics.append(Diagnostic("properties.textUsername", "Username cannot be empty [Location]",
                                              SeverityLevelEnum.Error))
            elif isBlank(component.properties.keyFilepath):
                diagnostics.append(
                    Diagnostic("properties.keyFilepath", "Private key file path cannot be empty [Location]",
                               SeverityLevelEnum.Error))

        if component.properties.readFromSource == "dbtable":
            if isBlank(component.properties.dbtable):
                diagnostics.append(
                    Diagnostic("properties.dbtable", "Table cannot be empty [Location]", SeverityLevelEnum.Error))
        elif component.properties.readFromSource == "query":
            if isBlank(component.properties.query):
                diagnostics.append(
                    Diagnostic("properties.query", "Query cannot be empty [Location]", SeverityLevelEnum.Error))

        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        return newState

    class SnowflakeFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: snowflake.SnowflakeProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            options = dict()

            sf_role = self.props.role if self.props.role is not None else ""
            if self.props.credType == "databricksSecrets":
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(spark)
                options = {"sfUrl": self.props.url,
                           "sfUser": dbutils.secrets.get(scope=self.props.credentialScope, key="username"),
                           "sfPassword": dbutils.secrets.get(scope=self.props.credentialScope, key="password"),
                           "sfDatabase": self.props.database,
                           "sfSchema": self.props.schemaName,
                           "sfWarehouse": self.props.warehouse,
                           "sfRole": sf_role
                           }
            elif self.props.credType == "userPwd":
                options = {"sfUrl": self.props.url,
                           "sfUser": self.props.textUsername,
                           "sfPassword": self.props.textPassword,
                           "sfDatabase": self.props.database,
                           "sfSchema": self.props.schemaName,
                           "sfWarehouse": self.props.warehouse,
                           "sfRole": sf_role
                           }
            elif self.props.credType == "userP8":
                import re
                from cryptography.hazmat.backends import default_backend
                from cryptography.hazmat.primitives import serialization
                p_key = serialization.load_pem_private_key(
                    spark.sparkContext.wholeTextFiles(self.props.keyFilepath).first()[1].encode(),
                    password=self.props.keyPasskey.encode()
                    if self.props.keyPasskey is not None and self.props.keyPasskey != "" else None,
                    backend=default_backend()
                )
                pkb = p_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat(self.props.privateKeyFormat),
                    encryption_algorithm=serialization.NoEncryption()
                ).decode("UTF-8")
                options = {"sfUrl": self.props.url,
                           "sfUser": self.props.textUsername,
                           "sfDatabase": self.props.database,
                           "sfSchema": self.props.schemaName,
                           "sfWarehouse": self.props.warehouse,
                           "pem_private_key": re.sub("-*(BEGIN|END) PRIVATE KEY-*\n", "", pkb).replace("\n", ""),
                           "sfRole": sf_role
                           }
            reader = spark.read.format("snowflake").options(**options)

            if self.props.readFromSource == "dbtable":
                reader = reader.option("dbtable", self.props.dbtable)
            elif self.props.readFromSource == "query":
                reader = reader.option("query", self.props.query)

            return reader.load()

        def targetApply(self, spark: SparkSession, in0: DataFrame):

            sf_role = self.props.role if self.props.role is not None else ""
            options: SubstituteDisabled = dict()
            if self.props.credType == "databricksSecrets":
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(spark)
                options = {
                    "sfUrl": self.props.url,
                    "sfUser": dbutils.secrets.get(scope=self.props.credentialScope, key="username"),
                    "sfPassword": dbutils.secrets.get(scope=self.props.credentialScope, key="password"),
                    "sfDatabase": self.props.database,
                    "sfSchema": self.props.schemaName,
                    "sfWarehouse": self.props.warehouse,
                    "sfRole": sf_role
                }
            elif self.props.credType == "userPwd":
                options = {
                    "sfUrl": self.props.url,
                    "sfUser": self.props.textUsername,
                    "sfPassword": self.props.textPassword,
                    "sfDatabase": self.props.database,
                    "sfSchema": self.props.schemaName,
                    "sfWarehouse": self.props.warehouse,
                    "sfRole": sf_role
                }
            elif self.props.credType == "userP8":
                import re
                from cryptography.hazmat.backends import default_backend
                from cryptography.hazmat.primitives import serialization
                p_key = serialization.load_pem_private_key(
                    spark.sparkContext.wholeTextFiles(self.props.keyFilepath).first()[1].encode(),
                    password=self.props.keyPasskey.encode()
                    if self.props.keyPasskey is not None and self.props.keyPasskey != "" else None,
                    backend=default_backend()
                )
                pkb = p_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat(self.props.privateKeyFormat),
                    encryption_algorithm=serialization.NoEncryption()
                ).decode("UTF-8")
                options = {
                    "sfUrl": self.props.url,
                    "sfUser": self.props.textUsername,
                    "sfDatabase": self.props.database,
                    "sfSchema": self.props.schemaName,
                    "sfWarehouse": self.props.warehouse,
                    "pem_private_key": re.sub("-*(BEGIN|END) PRIVATE KEY-*\n", "", pkb).replace("\n", ""),
                    "sfRole": sf_role
                }
            writer: SubstituteDisabled = in0.write.format("snowflake").options(**options)

            if self.props.dbtable is not None:
                writer = writer.option("dbtable", self.props.dbtable)

            if self.props.writeMode is not None:
                writer = writer.mode(self.props.writeMode)

            # todo @ank figure out an alternate for Utils.runQuery in python
            # also look at https://community.snowflake.com/s/question/0D50Z00008FNV6qSAH/running-dml-from-python-on-spark-azure-databricks

            if self.props.shouldPostSql:
                jvm = spark.sparkContext._jvm
                optionsJavaMap = jvm.PythonUtils.toScalaMap(options)
                jvm.net.snowflake.spark.snowflake.Utils.runQuery(optionsJavaMap, self.props.postSql)
            writer.save()
