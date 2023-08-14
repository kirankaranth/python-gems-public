from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from prophecy.cb.server.base.ComponentBuilderBase import (
    ComponentCode,
    Diagnostic,
    SeverityLevelEnum,
)
from prophecy.cb.server.base.DatasetBuilderBase import (
    DatasetSpec,
    DatasetProperties,
    Component,
)
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.StringUtils import isBlank
from prophecy.cb.server.base import WorkflowContext

class salesforce(DatasetSpec):
    name: str = "salesforce"
    datasetType: str = "WebApp"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/source-target/warehouse/salesforce"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class SalesforceProperties(DatasetProperties):
        schema: Optional[StructType] = None
        description: Optional[str] = ""
        credType: str = "databricksSecrets"
        credentialScope: Optional[str] = None
        textUsername: Optional[str] = None
        textPassword: Optional[str] = None
        loginURL: Optional[str] = None
        sfdatasetName: Optional[str] = None
        sfObject: Optional[str] = None
        metadataConfig: Optional[str] = None
        readFromSource: str = "soql"
        saql: Optional[str] = None
        soql: Optional[str] = None
        version: Optional[str] = None
        inferSchema: Optional[str] = None
        dateFormat: Optional[str] = None
        resultVariable: Optional[str] = None
        pageSize: Optional[str] = None
        upsert: Optional[str] = None
        bulk: Optional[str] = None
        pkChunking: Optional[str] = None
        chunkSize: Optional[str] = None
        timeout: Optional[str] = None
        maxCharsPerColumn: Optional[str] = None
        externalIdFieldName: Optional[str] = None
        queryAll: Optional[str] = None

    def sourceDialog(self) -> DatasetDialog:

        fieldPicker = FieldPicker(height=("100%")).addField(
            TextArea("Description", 2, placeholder="Dataset description..."),
            "description",
            True,
        )

        def addCommonFields(fp):
            return (
                fp.addField(Checkbox("Infer Schema (Optional)"), "inferSchema")
                    .addField(
                    TextBox("Date Format (Optional)").bindPlaceholder("MM/dd/yyyy"),
                    "dateFormat",
                )
                    .addField(
                    TextBox("Salesforce API Version (Optional)").bindPlaceholder(
                        "35.0"
                    ),
                    "version",
                )
            )

        saqlFieldPicker = addCommonFields(
            fieldPicker.addField(
                TextBox(
                    "Result variable used in SAQL query (Optional)"
                ).bindPlaceholder("q"),
                "resultVariable",
                True,
            ).addField(
                TextBox("Page size for each query (Optional)").bindPlaceholder("2000"),
                "pageSize",
                True,
            )
        )
        soqlFieldPicker = addCommonFields(
            fieldPicker.addField(Checkbox("Enable bulk query (Optional)"), "bulk")
                .addField(
                TextBox("Primary key chunking (Optional)").bindPlaceholder("100000"),
                "pkChunking",
                True,
            )
                .addField(
                TextBox("Chunk size (Optional)").bindPlaceholder("100000"),
                "chunkSize",
                True,
            )
                .addField(
                TextBox("Timeout (Optional)").bindPlaceholder("100"), "timeout", True
            )
                .addField(
                TextBox("Max Length of column (Optional)").bindPlaceholder("4096"),
                "maxCharsPerColumn  ",
                True,
            )
                .addField(
                TextBox(
                    "External ID field name for Salesforce Object (Optional)"
                ).bindPlaceholder("Id"),
                "externalIdFieldName",
                True,
            )
                .addField(
                Checkbox("Retrieve deleted and archived records (Optional)"), "queryAll"
            )
        )

        return (
            DatasetDialog("salesforce")
                .addSection(
                "LOCATION",
                ColumnsLayout().addColumn(
                    StackLayout(direction=("vertical"), gap=("1rem"))
                        .addElement(TitleElement(title="Credentials"))
                        .addElement(
                        StackLayout()
                            .addElement(
                            RadioGroup("Credentials")
                                .addOption("Databricks Secrets", "databricksSecrets")
                                .addOption("Username & Password", "userPwd")
                                .bindProperty("credType")
                        )
                            .addElement(
                            Condition()
                                .ifEqual(
                                PropExpr("component.properties.credType"),
                                StringExpr("databricksSecrets"),
                            )
                                .then(Credentials("").bindProperty("credentialScope"))
                                .otherwise(
                                ColumnsLayout(gap=("1rem"))
                                    .addColumn(
                                    TextBox("Username")
                                        .bindPlaceholder("username")
                                        .bindProperty("textUsername")
                                )
                                    .addColumn(
                                    TextBox("Password")
                                        .isPassword()
                                        .bindPlaceholder("password")
                                        .bindProperty("textPassword")
                                )
                            )
                        )
                    )
                        .addElement(TitleElement(title="URL"))
                        .addElement(
                        TextBox("Login URL (Optional)")
                            .bindPlaceholder("https://login.salesforce.com")
                            .bindProperty("loginURL")
                    )
                        .addElement(
                        StackLayout()
                            .addElement(
                            RadioGroup("Data Source")
                                .addOption("SAQL", "saql")
                                .addOption("SOQL", "soql")
                                .bindProperty("readFromSource")
                        )
                            .addElement(
                            Condition()
                                .ifEqual(
                                PropExpr("component.properties.readFromSource"),
                                StringExpr("saql"),
                            )
                                .then(
                                TextBox("SAQL query to query Salesforce Wave")
                                    .bindPlaceholder("q = load 'OpsDates1';")
                                    .bindProperty("saql")
                            )
                                .otherwise(
                                TextBox("SOQL query to query Salesforce Object")
                                    .bindPlaceholder("SELECT Name FROM Account")
                                    .bindProperty("soql")
                            )
                        )
                    )
                ),
            )
                .addSection(
                "PROPERTIES",
                ColumnsLayout(gap=("1rem"), height=("100%"))
                    .addColumn(
                    ScrollBox().addElement(
                        StackLayout(height=("100%")).addElement(
                            StackItem(grow=(1)).addElement(
                                Condition()
                                    .ifEqual(
                                    PropExpr("component.properties.readFromSource"),
                                    StringExpr("saql"),
                                )
                                    .then(saqlFieldPicker)
                                    .otherwise(soqlFieldPicker)
                            )
                        )
                    ),
                    "auto",
                )
                    .addColumn(SchemaTable("").isReadOnly().bindProperty("schema"), "5fr"),
            )
                .addSection("PREVIEW", PreviewTable("").bindProperty("schema"))
        )

    def targetDialog(self) -> DatasetDialog:
        fieldPicker = (
            FieldPicker(height=("100%"))
                .addField(
                TextBox("SF object to be updated (Optional)").bindPlaceholder(
                    "Contact"
                ),
                "sfObject",
                True,
            )
                .addField(
                TextBox(
                    "Name of the dataset to be created in Salesforce Wave"
                ).bindPlaceholder("your_dataset_name"),
                "sfdatasetName",
                True,
            )
                .addField(
                TextArea("Metadata configuration in json (Optional)", 2, placeholder="""{
  "<df_data_type>": {
  "wave_type": "<wave_data_type>",
  "precision": "<precision>",
  "scale": "<scale>",
  "format": "<format>",
  "defaultValue": "<defaultValue>"
  }
}"""),
                "metadataConfig",
                True,
            )
                .addField(
                Checkbox(" Flag to upsert data to Salesforce (Optional)"), "upsert"
            )
                .addField(
                TextBox(
                    "External ID field name for Salesforce Object (Optional)"
                ).bindPlaceholder("Id"),
                "externalIdFieldName",
                True,
            )
        )

        return (
            DatasetDialog("salesforce")
                .addSection(
                "LOCATION",
                ColumnsLayout().addColumn(
                    StackLayout(direction=("vertical"), gap=("1rem"))
                        .addElement(TitleElement(title="Credentials"))
                        .addElement(
                        StackLayout()
                            .addElement(
                            RadioGroup("Credentials")
                                .addOption("Databricks Secrets", "databricksSecrets")
                                .addOption("Username & Password", "userPwd")
                                .bindProperty("credType")
                        )
                            .addElement(
                            Condition()
                                .ifEqual(
                                PropExpr("component.properties.credType"),
                                StringExpr("databricksSecrets"),
                            )
                                .then(Credentials("").bindProperty("credentialScope"))
                                .otherwise(
                                ColumnsLayout(gap=("1rem"))
                                    .addColumn(
                                    TextBox("Username")
                                        .bindPlaceholder("username")
                                        .bindProperty("textUsername")
                                )
                                    .addColumn(
                                    TextBox("Password")
                                        .isPassword()
                                        .bindPlaceholder("password")
                                        .bindProperty("textPassword")
                                )
                            )
                        )
                    )
                        .addElement(TitleElement(title="URL"))
                        .addElement(
                        TextBox("Login URL (Optional)")
                            .bindPlaceholder("https://login.salesforce.com")
                            .bindProperty("loginURL")
                    )
                ),
            )
                .addSection(
                "PROPERTIES",
                ColumnsLayout(gap=("1rem"), height=("100%"))
                    .addColumn(
                    ScrollBox().addElement(
                        StackLayout(height=("100%")).addElement(
                            StackItem(grow=(1)).addElement(fieldPicker)
                        )
                    ),
                    "auto",
                )
                    .addColumn(SchemaTable("").isReadOnly().bindProperty("schema"), "5fr"),
            )
        )

    def validate(self, context: WorkflowContext, component: Component) -> list:
        diagnostics = super(salesforce, self).validate(context, component)

        if component.properties.credType == "databricksSecrets":
            if isBlank(component.properties.credentialScope):
                diagnostics.append(
                    Diagnostic(
                        "properties.credentialScope",
                        "Credential Scope cannot be empty [Location]",
                        SeverityLevelEnum.Error,
                    )
                )
        elif component.properties.credType == "userPwd":
            if isBlank(component.properties.textUsername):
                diagnostics.append(
                    Diagnostic(
                        "properties.textUsername",
                        "Username cannot be empty [Location]",
                        SeverityLevelEnum.Error,
                    )
                )
            elif isBlank(component.properties.textPassword):
                diagnostics.append(
                    Diagnostic(
                        "properties.textPassword",
                        "Password cannot be empty [Location]",
                        SeverityLevelEnum.Error,
                    )
                )

        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        return newState

    class SalesforceFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: salesforce.SalesforceProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            reader = spark.read.format("com.springml.spark.salesforce")
            if self.props.credType == "databricksSecrets":
                from pyspark.dbutils import DBUtils

                dbutils = DBUtils(spark)
                reader = reader.option(
                    "username",
                    dbutils.secrets.get(
                        scope=self.props.credentialScope, key="username"
                    ),
                )
                reader = reader.option(
                    "password",
                    dbutils.secrets.get(
                        scope=self.props.credentialScope, key="password"
                    ),
                )
            elif self.props.credType == "userPwd":
                reader = reader.option("username", f"{self.props.textUsername}")
                reader = reader.option("password", f"{self.props.textPassword}")

            if self.props.loginURL is not None:
                reader = reader.option("login", self.props.loginURL)

            if self.props.inferSchema is not None:
                reader = reader.option("inferSchema", self.props.inferSchema)

            if self.props.dateFormat is not None:
                reader = reader.option("dateFormat", self.props.inferSchema)

            if self.props.readFromSource == "soql":
                reader = reader.option("soql", self.props.soql)

                if self.props.bulk is not None:
                    reader = reader.option("bulk", self.props.bulk)
                if self.props.pkChunking is not None:
                    reader = reader.option("pkChunking", self.props.pkChunking)
                if self.props.chunkSize is not None:
                    reader = reader.option("chunkSize", self.props.chunkSize)
                if self.props.timeout is not None:
                    reader = reader.option("timeout", self.props.timeout)
                if self.props.maxCharsPerColumn is not None:
                    reader = reader.option(
                        "maxCharsPerColumn", self.props.maxCharsPerColumn
                    )
                if self.props.externalIdFieldName is not None:
                    reader = reader.option(
                        "externalIdFieldName", self.props.externalIdFieldName
                    )
                if self.props.queryAll is not None:
                    reader = reader.option("queryAll", self.props.queryAll)

            if self.props.readFromSource == "saql":
                reader = reader.option("saql", self.props.saql)

                if self.props.resultVariable is not None:
                    reader = reader.option("resultVariable", self.props.resultVariable)
                if self.props.pageSize is not None:
                    reader = reader.option("pageSize", self.props.pageSize)

            return reader.load()

        def targetApply(self, spark: SparkSession, in0: DataFrame):
            writer = in0.write.format("com.springml.spark.salesforce")

            if self.props.credType == "databricksSecrets":
                from pyspark.dbutils import DBUtils

                dbutils = DBUtils(spark)
                writer = writer.option(
                    "user",
                    dbutils.secrets.get(
                        scope=self.props.credentialScope, key="username"
                    ),
                )
                writer = writer.option(
                    "password",
                    dbutils.secrets.get(
                        scope=self.props.credentialScope, key="password"
                    ),
                )
            elif self.props.credType == "userPwd":
                writer = writer.option("user", f"{self.props.textUsername}")
                writer = writer.option("password", f"{self.props.textPassword}")

            if self.props.loginURL is not None:
                writer = writer.option("login", self.props.loginURL)

            if self.props.sfObject is not None:
                writer = writer.option("sfObject", self.props.sfObject)

            if self.props.sfdatasetName is not None:
                writer = writer.option("datasetName", self.props.sfdatasetName)

            if self.props.metadataConfig is not None:
                writer = writer.option("metadataConfig", self.props.metadataConfig)

            if self.props.upsert is not None:
                writer = writer.option("upsert", self.props.upsert)

            if self.props.externalIdFieldName is not None:
                writer = writer.option(
                    "externalIdFieldName", self.props.externalIdFieldName
                )

            writer.save()