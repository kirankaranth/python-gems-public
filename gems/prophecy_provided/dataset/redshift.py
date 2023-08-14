from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *

from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode, Diagnostic, SeverityLevelEnum, \
    SubstituteDisabled
from prophecy.cb.server.base.DatasetBuilderBase import DatasetSpec, DatasetProperties, Component
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.StringUtils import isBlank
from prophecy.cb.server.base import WorkflowContext

class redshift(DatasetSpec):
    name: str = "redshift"
    datasetType: str = "Warehouse"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/source-target/warehouse/redshift"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class RedshiftProperties(DatasetProperties):
        schema: Optional[StructType] = None
        description: Optional[str] = ""
        credType: str = "databricksSecrets"
        credentialScope: Optional[str] = None
        textUsername: Optional[str] = None
        textPassword: Optional[str] = None
        jdbcUrl: str = ""
        readFromSource: str = "dbtable"
        dbtable: Optional[str] = None
        query: Optional[str] = None
        driver: Optional[str] = None
        writeMode: Optional[str] = None
        jdbcTempDir: Optional[str] = None
        forward_spark_s3_credentials: Optional[bool] = None
        aws_iam_role: Optional[str] = None
        temporary_aws_access_key_id: Optional[bool] = None
        diststyle: Optional[str] = None
        distkey: Optional[str] = None
        sqlPreAction: Optional[SColumn] = None
        sqlPostAction: Optional[SColumn] = None
        str_max_length: Optional[str] = None
        secrets_name_user: Optional[str] = None
        secrets_name_password: Optional[str] = None

    def sourceDialog(self) -> DatasetDialog:
        return DatasetDialog("redshift") \
            .addSection(
            "LOCATION",
            ColumnsLayout()
                .addColumn(
                StackLayout(direction=("vertical"), gap=("1rem"))
                    .addElement(TitleElement(title = "Credentials"))
                    .addElement(
                        StackLayout()
                                .addElement(TextBox("Secrets Scope").bindPlaceholder("scope").bindProperty("credentialScope"))
                                .addElement(TextBox("User Secret Name").bindPlaceholder("username_secret_name").bindProperty("secrets_name_user"))
                                .addElement(TextBox("Password Secret Name").bindPlaceholder("password_secret_name").bindProperty("secrets_name_password"))
                    )
                    .addElement(TitleElement(title="URL"))
                    .addElement(
                    TextBox("JDBC URL")
                        .bindPlaceholder("jdbc:redshift://<jdbcHostname>:<jdbcPort>/<jdbcDatabase>")
                        .bindProperty("jdbcUrl")
                ).addElement(
                    TextBox("Temporary Directory").bindPlaceholder("s3a://redshift_bucket").bindProperty("jdbcTempDir"))
                    #            .addElement(TitleElement(title = "Table"))
                    .addElement(
                    StackLayout()
                        .addElement(
                        RadioGroup("Data Source")
                            .addOption("DB Table", "dbtable")
                            .addOption("SQL Query", "sqlQuery")
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
            ColumnsLayout(gap=("1rem"), height=("100%"))
                .addColumn(
                ScrollBox().addElement(
                    StackLayout(height=("100%"))
                        .addElement(
                        Checkbox("Forward S3 access credentials to databricks").bindProperty(
                            "forward_spark_s3_credentials")
                    )
                        .addElement(
                        StackItem(grow=(1)).addElement(
                            FieldPicker(height=("100%"))
                                .addField(TextBox("Driver").bindPlaceholder("com.amazon.redshift.jdbc42.Driver"),
                                          "driver")
                                .addField(TextBox("AWS IAM Role").bindPlaceholder(
                                "arn:aws:iam::123456789000:role/<redshift-iam-role>"), "aws_iam_role")
                                .addField(Checkbox("Temporary AWS access key id"), "temporary_aws_access_key_id")
                        )
                    )
                ),
                "400px"
            )
                .addColumn(SchemaTable("").isReadOnly().bindProperty("schema"), "5fr")
        ) \
            .addSection(
            "PREVIEW",
            PreviewTable("").bindProperty("schema"))

    def targetDialog(self) -> DatasetDialog:
        return DatasetDialog("redshift") \
            .addSection(
            "LOCATION",
            ColumnsLayout()
                .addColumn(
                StackLayout(direction=("vertical"), gap=("1rem"))
                    .addElement(TitleElement(title = "Credentials"))
                    .addElement(
                        StackLayout()
                            .addElement(TextBox("Secrets Scope").bindPlaceholder("scope").bindProperty("credentialScope"))
                            .addElement(TextBox("User Secret Name").bindPlaceholder("username_secret_name").bindProperty("secrets_name_user"))
                            .addElement(TextBox("Password Secret Name").bindPlaceholder("password_secret_name").bindProperty("secrets_name_password"))
                )
                    .addElement(TitleElement(title="URL"))
                    .addElement(
                    TextBox("JDBC URL")
                        .bindPlaceholder("jdbc:redshift://<jdbcHostname>:<jdbcPort>/<jdbcDatabase>")
                        .bindProperty("jdbcUrl")
                )
                    .addElement(TextBox("Temporary Directory").bindPlaceholder("s3a://redshift_bucket/").bindProperty(
                    "jdbcTempDir"))
                    .addElement(TitleElement(title="Table"))
                    .addElement(TextBox("Table").bindPlaceholder("tablename").bindProperty("dbtable"))
            )
        ) \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap=("1rem"), height=("100%"))
                .addColumn(
                ScrollBox().addElement(
                    StackLayout(height=("100%"))
                        .addElement(
                        Checkbox("Forward S3 access credentials to databricks").bindProperty(
                            "forward_spark_s3_credentials")
                    ).addElement(SelectBox("Write Mode")
                                 .addOption("error", "error")
                                 .addOption("overwrite", "overwrite")
                                 .addOption("append", "append")
                                 .addOption("ignore", "ignore").bindProperty("writeMode"))
                        .addElement(
                        StackItem(grow=(1)).addElement(
                            FieldPicker(height=("100%"))
                                .addField(TextBox("Driver").bindPlaceholder("org.amazon.redshift.jdbc42.Driver"),
                                          "driver")
                                .addField(TextBox("AWS IAM Role").bindPlaceholder(
                                "arn:aws:iam::123456789000:role/<redshift-iam-role>"), "aws_iam_role")
                                .addField(Checkbox("Temporary AWS access key id"), "temporary_aws_access_key_id")
                                .addField(TextBox("Max length for string columns in redshift").bindPlaceholder("2048"),
                                          "str_max_length")
                                .addField(
                                SelectBox("Row distribution style for new table")
                                    .addOption("EVEN", "EVEN")
                                    .addOption("KEY", "KEY")
                                    .addOption("ALL", "ALL")
                                , "diststyle"
                            )
                                .addField(TextBox("Distribution key for new table").bindPlaceholder(""), "distkey")
                        )
                    )
                ),
                "400px"
            )
                .addColumn(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"), "5fr")
        )

    def validate(self, context: WorkflowContext, component: Component) -> list:
        diagnostics = super(redshift, self).validate(context, component)

        if component.properties.credType == "databricksSecrets":
            if isBlank(component.properties.credentialScope):
                diagnostics.append(Diagnostic(
                    "properties.credentialScope",
                    "Credential Scope cannot be empty [Location]",
                    SeverityLevelEnum.Error))
            if isBlank(component.properties.secrets_name_user):
                diagnostics.append(Diagnostic(
                    "properties.secrets_name_user",
                    "Credential User Secret Name cannot be empty [Location]",
                    SeverityLevelEnum.Error))
            if isBlank(component.properties.secrets_name_password):
                diagnostics.append(Diagnostic(
                    "properties.secrets_name_password",
                    "Credential Password Secret Name cannot be empty [Location]",
                    SeverityLevelEnum.Error))
        elif component.properties.credType == "userPwd":
            diagnostics.append(Diagnostic("properties.textPassword",
                "Plaintext usernames/passwords are deprecated and will be removed in a future version.", SeverityLevelEnum.Warning))
            if isBlank(component.properties.textUsername):
                diagnostics.append(Diagnostic("properties.textUsername", "Username cannot be empty [Location]",
                                              SeverityLevelEnum.Error))
            elif isBlank(component.properties.textPassword):
                diagnostics.append(Diagnostic("properties.textPassword", "Password cannot be empty [Location]",
                                              SeverityLevelEnum.Error))

        if isBlank(component.properties.jdbcUrl):
            diagnostics.append(
                Diagnostic("properties.jdbcUrl", "JDBC URL cannot be empty [Location]", SeverityLevelEnum.Error))

        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        return newState

    class RedshiftFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: redshift.RedshiftProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            reader = spark.read.format("com.databricks.spark.redshift")
            reader = reader.option("url", self.props.jdbcUrl)
            if self.props.credType == "databricksSecrets":
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(spark)
                reader = reader.option("user", dbutils.secrets.get(scope=self.props.credentialScope, key=f"{self.props.secrets_name_user}"))
                reader = reader.option("password",
                                       dbutils.secrets.get(scope=self.props.credentialScope, key=f"{self.props.secrets_name_password}"))
            elif self.props.credType == "userPwd":
                reader = reader.option("user", f"{self.props.textUsername}")
                reader = reader.option("password", f"{self.props.textPassword}")

            if self.props.readFromSource == "dbtable":
                reader = reader.option("dbtable", self.props.dbtable)
            elif self.props.readFromSource == "sqlQuery":
                reader = reader.option("query", self.props.query)

            if self.props.aws_iam_role is not None:
                reader = reader.option("aws_iam_role", self.props.aws_iam_role)
            if self.props.forward_spark_s3_credentials is not None:
                reader = reader.option("forward_spark_s3_credentials", self.props.forward_spark_s3_credentials)
            if self.props.jdbcTempDir is not None:
                reader = reader.option("tempdir", self.props.jdbcTempDir)
            if self.props.jdbcUrl is not None:
                reader = reader.option("url", self.props.jdbcUrl)
            if self.props.driver is not None:
                reader = reader.option("jdbcdriver", self.props.driver)
            if self.props.temporary_aws_access_key_id is not None:
                reader = reader.option("temporary_aws_access_key_id", self.props.temporary_aws_access_key_id)

            return reader.load()

        def targetApply(self, spark: SparkSession, in0: DataFrame):

            username = self.props.textUsername
            password = self.props.textPassword
            if self.props.credType == "databricksSecrets":
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(spark)
                username = dbutils.secrets.get(scope=self.props.credentialScope, key=f"{self.props.secrets_name_user}")
                password = dbutils.secrets.get(scope=self.props.credentialScope, key=f"{self.props.secrets_name_password}")

            df = in0
            if self.props.str_max_length is not None:
                meta_dict = {"maxlength": int(self.props.str_max_length)}
                fields = []
                old_schema: SubstituteDisabled = in0.schema
                for col in in0.columns:
                    if old_schema[col].dataType == "StringType":
                        fields.append(StructField(col, eval(old_schema[col].dataType + "()"),
                                                  metadata=meta_dict))
                    else:
                        fields.append(old_schema[col])
                new_schema: SubstituteDisabled = StructType(fields)
                df = spark.createDataFrame(in0.rdd, new_schema)

            writer = df.write.format("com.databricks.spark.redshift")

            writer = writer.option("url", self.props.jdbcUrl)

            if username is not None:
                writer = writer.option("user", username).option("password", password)

            if self.props.dbtable is not None:
                writer = writer.option("dbtable", self.props.dbtable)

            if self.props.aws_iam_role is not None:
                writer = writer.option("aws_iam_role", self.props.aws_iam_role)

            if self.props.forward_spark_s3_credentials is not None:
                writer = writer.option("forward_spark_s3_credentials", self.props.forward_spark_s3_credentials)
            if self.props.jdbcTempDir is not None:
                writer = writer.option("tempdir", self.props.jdbcTempDir)
            if self.props.jdbcUrl is not None:
                writer = writer.option("url", self.props.jdbcUrl)
            if self.props.driver is not None:
                writer = writer.option("jdbcdriver", self.props.driver)
            if self.props.temporary_aws_access_key_id is not None:
                writer = writer.option("temporary_aws_access_key_id", self.props.temporary_aws_access_key_id)
            if self.props.distkey is not None:
                writer = writer.option("distkey", self.props.distkey)
            if self.props.diststyle is not None:
                writer = writer.option("queryTimeout", self.props.diststyle)

            if self.props.writeMode is not None:
                writer = writer.mode(self.props.writeMode)

            writer.save()