from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode, Diagnostic, SeverityLevelEnum
from prophecy.cb.server.base.DatasetBuilderBase import DatasetSpec, DatasetProperties, Component
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.NumberUtils import parseInt
from prophecy.cb.util.StringUtils import isBlank
from prophecy.cb.server.base import WorkflowContext

class jdbc(DatasetSpec):
    name: str = "jdbc"
    datasetType: str = "Warehouse"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/source-target/warehouse/jdbc"

    def optimizeCode(self) -> bool:
        return True

    # todo
    #  These 4 props are defined but not used in any binding in the dialogs: customSchema, keytab, principal, refreshKrb5Config
    @dataclass(frozen=True)
    class JDBCProperties(DatasetProperties):
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
        driver: str = ""
        partitionColumn: Optional[str] = None
        lowerBound: Optional[str] = None
        upperBound: Optional[str] = None
        numPartitions: Optional[str] = None
        queryTimeout: Optional[str] = None
        fetchsize: Optional[str] = None
        batchsize: Optional[str] = None
        isolationLevel: Optional[str] = None
        sessionInitStatement: Optional[str] = None
        truncate: Optional[bool] = None
        cascadeTruncate: Optional[bool] = None
        createTableOptions: Optional[str] = None
        createTableColumnTypes: Optional[str] = None
        customSchema: Optional[str] = None
        pushDownPredicate: Optional[bool] = True
        pushDownAggregate: Optional[bool] = None
        keytab: Optional[str] = None
        principal: Optional[str] = None
        refreshKrb5Config: Optional[bool] = None
        writeMode: Optional[str] = None

    def sourceDialog(self) -> DatasetDialog:

        fieldPicker = FieldPicker(height=("100%")) \
            .addField(
            TextArea("Description", 2, placeholder="Dataset description..."),
            "description",
            True
        )

        def addCommonFields(fp):
            return fp.addField(TextBox("Driver").bindPlaceholder("org.postgresql.Driver"), "driver", True) \
                .addField(TextBox("Number of Partitions").bindPlaceholder(""), "numPartitions") \
                .addField(TextBox("Query Timeout").bindPlaceholder(""), "queryTimeout") \
                .addField(TextBox("Fetch Size").bindPlaceholder(""), "fetchsize") \
                .addField(
                # todo check what to use in place of text box here
                TextBox("Session Init Statement")
                    .bindPlaceholder(
                    "BEGIN execute immediate 'alter session set \"_serial_direct_read\"=true'; END;"
                ),
                "sessionInitStatement"
            ) \
                .addField(Checkbox("Push-down Predicate"), "pushDownPredicate") \
                .addField(Checkbox("Push-down Aggregate"), "pushDownAggregate")

        sqlQueryFieldPicker = addCommonFields(fieldPicker)
        dbtableFieldPicker = addCommonFields(
            fieldPicker.addField(
                SchemaColumnsDropdown("Partition Column")
                    .bindSchema("schema")
                    .showErrorsFor("partitionColumn")
                    .allowClearSelection(),
                "partitionColumn",
                True
            ).addField(TextBox("Lower Bound").bindPlaceholder(""), "lowerBound", True)
                .addField(TextBox("Upper Bound").bindPlaceholder(""), "upperBound", True)
        )

        return DatasetDialog("jdbc") \
            .addSection(
            "LOCATION",
            ColumnsLayout()
                .addColumn(
                StackLayout(direction=("vertical"), gap=("1rem"))
                    #            .addElement(TitleElement(title = "Credentials"))
                    .addElement(
                    StackLayout()
                        .addElement(
                        RadioGroup("Credentials")
                            .addOption("Databricks Secrets", "databricksSecrets")
                            .addOption("Username & Password", "userPwd")
                            .addOption("Environment variables", "userPwdEnv")
                            .bindProperty("credType")
                    )
                        .addElement(
                        Condition()
                            .ifEqual(PropExpr("component.properties.credType"), StringExpr("databricksSecrets"))
                            .then(Credentials("").bindProperty("credentialScope"))
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
                    .addElement(TitleElement(title="URL"))
                    .addElement(
                    TextBox("JDBC URL")
                        .bindPlaceholder("jdbc:<sqlserver>://<jdbcHostname>:<jdbcPort>/<jdbcDatabase>")
                        .bindProperty("jdbcUrl")
                )
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
                        StackItem(grow=(1)).addElement(
                            Condition()
                                .ifEqual(PropExpr("component.properties.readFromSource"), StringExpr("dbtable"))
                                .then(dbtableFieldPicker)
                                .otherwise(sqlQueryFieldPicker)
                        )
                    )
                ),
                "auto"
            )
                .addColumn(SchemaTable("").isReadOnly().bindProperty("schema"), "5fr")
        ) \
            .addSection(
            "PREVIEW",
            PreviewTable("").bindProperty("schema"))

    def targetDialog(self) -> DatasetDialog:
        return DatasetDialog("jdbc") \
            .addSection(
            "LOCATION",
            ColumnsLayout()
                .addColumn(
                StackLayout(direction=("vertical"), gap=("1rem"))
                    #            .addElement(TitleElement(title = "Credentials"))
                    .addElement(
                    StackLayout()
                        .addElement(
                        RadioGroup("Credentials")
                            .addOption("Databricks Secrets", "databricksSecrets")
                            .addOption("Username & Password", "userPwd")
                            .addOption("Environment variables", "userPwdEnv")
                        .bindProperty("credType")
                    )
                        .addElement(
                        Condition()
                            .ifEqual(PropExpr("component.properties.credType"), StringExpr("databricksSecrets"))
                            .then(Credentials("").bindProperty("credentialScope"))
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
                    .addElement(TitleElement(title="URL"))
                    .addElement(
                    TextBox("JDBC URL")
                        .bindPlaceholder("jdbc:<sqlserver>://<jdbcHostname>:<jdbcPort>/<jdbcDatabase>")
                        .bindProperty("jdbcUrl")
                )
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
                        StackItem(grow=(1)).addElement(
                            FieldPicker(height=("100%"))
                                .addField(
                                TextArea("Description", 2, placeholder="Dataset description..."),
                                "description",
                                True
                            ).addField(
                                TextBox("Driver").bindPlaceholder("org.postgresql.Driver"), "driver", True
                            ).addField(
                                SelectBox("Write Mode")
                                    .addOption("error", "error")
                                    .addOption("overwrite", "overwrite")
                                    .addOption("append", "append")
                                    .addOption("ignore", "ignore"),
                                "writeMode"
                            )
                                .addField(TextBox("Number of Partitions").bindPlaceholder(""), "numPartitions")
                                .addField(TextBox("Query Timeout").bindPlaceholder(""), "queryTimeout")
                                .addField(TextBox("Batch Size").bindPlaceholder(""), "batchsize")
                                .addField(
                                SelectBox("Isolation Level")
                                    .addOption("READ_UNCOMMITTED", "READ_UNCOMMITTED")
                                    .addOption("NONE", "NONE")
                                    .addOption("READ_COMMITTED", "READ_COMMITTED")
                                    .addOption("REPEATABLE_READ", "REPEATABLE_READ")
                                    .addOption("SERIALIZABLE", "SERIALIZABLE"),
                                "isolationLevel"
                            )
                                .addField(Checkbox("Truncate"), "truncate")
                                # todo this one's default is dependent on jdbc dialect.
                                .addField(Checkbox("Cascade Truncate"), "cascadeTruncate")
                                # todo find example and bind that as placeholder
                                .addField(TextBox("Create Table Options"), "createTableOptions")
                                .addField(
                                TextBox("Create Table Column Types")
                                    .bindPlaceholder("name CHAR(64), comments VARCHAR(1024)"),
                                "createTableColumnTypes"
                            )
                            # skipping kerberos things for now
                        )
                    )
                ),
                "auto"
            )
                .addColumn(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"), "5fr")
        )

    def validate(self, context: WorkflowContext, component: Component) -> list:
        diagnostics = super(jdbc, self).validate(context, component)

        if component.properties.credType == "databricksSecrets":
            if isBlank(component.properties.credentialScope):
                diagnostics.append(Diagnostic(
                    "properties.credentialScope",
                    "Credential Scope cannot be empty [Location]",
                    SeverityLevelEnum.Error))
        elif component.properties.credType == "userPwd" or component.properties.credType == "userPwdEnv":
            if isBlank(component.properties.textUsername):
                diagnostics.append(Diagnostic("properties.textUsername", "Username cannot be empty [Location]",
                                              SeverityLevelEnum.Error))
            elif isBlank(component.properties.textPassword):
                diagnostics.append(Diagnostic("properties.textPassword", "Password cannot be empty [Location]",
                                              SeverityLevelEnum.Error))

            if component.properties.credType == "userPwd" and ( isBlank(component.properties.textPassword) or (not component.properties.textPassword.startswith("${"))):
                diagnostics.append(Diagnostic("properties.textPassword", "Storing plain-text passwords poses a "
                                                                         "security risk and is not recommended.",
                                              SeverityLevelEnum.Warning))

        if isBlank(component.properties.jdbcUrl):
            diagnostics.append(
                Diagnostic("properties.jdbcUrl", "JDBC URL cannot be empty [Location]", SeverityLevelEnum.Error))

        pc = component.properties.partitionColumn
        PC = None if (isBlank(pc)) else pc
        lb = component.properties.lowerBound
        LB = None if (isBlank(lb)) else lb
        ub = component.properties.upperBound
        UB = None if (isBlank(ub)) else ub

        if (PC is None and LB is None and UB is None):
            # none of the three is specified. This is okay
            # if these are not specified, run independent validation on numPartitions.
            if not isBlank(component.properties.numPartitions):
                numPartitionsInt = parseInt(component.properties.numPartitions)
                if numPartitionsInt is None:
                    diagnostics.append(Diagnostic(
                        "properties.numPartitions",
                        "Number of partitions has to be a Number [Properties]",
                        SeverityLevelEnum.Error
                    ))
        else:
            # full or partial specification.
            # if any one of these is specified the all 4 have to be specified
            # now, if any of them is missing, we gotta slap this error
            diagMsg = "Partition Columns, Lower Bound, Upper Bound: These options must all be specified if any of them is specified. In addition, Number of Partitions must be specified. [Properties]"
            if component.properties.numPartitions is None or isBlank(component.properties.numPartitions):
                diagnostics.append(Diagnostic("properties.numPartitions", diagMsg, SeverityLevelEnum.Error))
            else:
                numPartitionsInt = parseInt(component.properties.numPartitions)
                if numPartitionsInt is None:
                    diagnostics.append(Diagnostic(
                        "properties.numPartitions",
                        "Number of partitions has to be a Number [Properties]",
                        SeverityLevelEnum.Error
                    ))

            if (isBlank(PC)):
                diagnostics.append(Diagnostic("properties.partitionColumn", diagMsg, SeverityLevelEnum.Error)
                                   )
            if isBlank(LB):
                diagnostics.append(Diagnostic("properties.lowerBound", diagMsg, SeverityLevelEnum.Error))
            # else:
            #     if parseInt(LB) is None:
            #         diagnostics.append(Diagnostic("properties.lowerBound", "Lower Bound has to be a Long [Properties]",
            #                                       SeverityLevelEnum.Error)
            #                            )

            if isBlank(UB):
                diagnostics.append(Diagnostic("properties.upperBound", diagMsg, SeverityLevelEnum.Error))
            # else:
            #     if parseInt(UB) is None:
            #         diagnostics.append(Diagnostic("properties.upperBound", "Upper Bound has to be a Long [Properties]",
            #                                       SeverityLevelEnum.Error)
            #                            )

        if isBlank(component.properties.driver):
            diagnostics.append(
                Diagnostic("properties.driver", "JDBC Driver cannot be empty [Properties]", SeverityLevelEnum.Error)
            )

        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        return newState

    class JDBCFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: jdbc.JDBCProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            reader = spark.read.format("jdbc")
            reader = reader.option("url", self.props.jdbcUrl)
            if self.props.credType == "databricksSecrets":
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(spark)
                reader = reader.option("user", dbutils.secrets.get(scope=self.props.credentialScope, key="username"))
                reader = reader.option("password",
                                       dbutils.secrets.get(scope=self.props.credentialScope, key="password"))
            elif self.props.credType == "userPwd":
                reader = reader.option("user", f"{self.props.textUsername}")
                reader = reader.option("password", f"{self.props.textPassword}")
            elif self.props.credType == "userPwdEnv":
                import os
                reader = reader.option("user", os.environ[f"{self.props.textUsername}"])
                reader = reader.option("password", os.environ[f"{self.props.textPassword}"])

            if self.props.readFromSource == "dbtable":
                reader = reader.option("dbtable", self.props.dbtable)
            elif self.props.readFromSource == "sqlQuery":
                reader = reader.option("query", self.props.query)

            if self.props.partitionColumn is not None:
                reader = reader.option("partitionColumn", self.props.partitionColumn)
            if self.props.lowerBound is not None:
                reader = reader.option("lowerBound", self.props.lowerBound)
            if self.props.upperBound is not None:
                reader = reader.option("upperBound", self.props.upperBound)
            if self.props.numPartitions is not None:
                reader = reader.option("numPartitions", self.props.numPartitions)
            if self.props.queryTimeout is not None:
                reader = reader.option("queryTimeout", self.props.queryTimeout)
            if self.props.fetchsize is not None:
                reader = reader.option("fetchsize", self.props.fetchsize)
            if self.props.sessionInitStatement is not None:
                reader = reader.option("sessionInitStatement", self.props.sessionInitStatement)
            # if self.props.customSchema is not None:
            #     reader = reader.option("customSchema", self.props.customSchema)
            if self.props.pushDownPredicate is not None:
                reader = reader.option("pushDownPredicate", self.props.pushDownPredicate)
            if self.props.pushDownAggregate is not None:
                reader = reader.option("pushDownAggregate", self.props.pushDownAggregate)
            if self.props.driver is not None:
                reader = reader.option("driver", self.props.driver)

            return reader.load()

        def targetApply(self, spark: SparkSession, in0: DataFrame):
            writer = in0.write.format("jdbc")

            writer = writer.option("url", self.props.jdbcUrl)
            if self.props.dbtable is not None:
                writer = writer.option("dbtable", self.props.dbtable)
            if self.props.credType == "databricksSecrets":
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(spark)
                writer = writer.option("user", dbutils.secrets.get(scope=self.props.credentialScope, key="username"))
                writer = writer.option("password",
                                       dbutils.secrets.get(scope=self.props.credentialScope, key="password"))
            elif self.props.credType == "userPwd":
                writer = writer.option("user", f"{self.props.textUsername}")
                writer = writer.option("password", f"{self.props.textPassword}")
            elif self.props.credType == "userPwdEnv":
                import os
                writer = writer.option("user", os.environ[f"{self.props.textUsername}"])
                writer = writer.option("password", os.environ[f"{self.props.textPassword}"])

            if self.props.numPartitions is not None:
                writer = writer.option("numPartitions", self.props.numPartitions)
            if self.props.queryTimeout is not None:
                writer = writer.option("queryTimeout", self.props.queryTimeout)
            if self.props.batchsize is not None:
                writer = writer.option("batchsize", self.props.batchsize)
            if self.props.isolationLevel is not None:
                writer = writer.option("isolationLevel", self.props.isolationLevel)
            if self.props.truncate is not None:
                writer = writer.option("truncate", self.props.truncate)
            if self.props.cascadeTruncate is not None:
                writer = writer.option("cascadeTruncate", self.props.cascadeTruncate)
            if self.props.createTableOptions is not None:
                writer = writer.option("createTableOptions", self.props.createTableOptions)
            if self.props.createTableColumnTypes is not None:
                writer = writer.option("createTableColumnTypes", self.props.createTableColumnTypes)
            if self.props.driver is not None:
                writer = writer.option("driver", self.props.driver)

            if self.props.writeMode is not None:
                writer = writer.mode(self.props.writeMode)

            writer.save()
