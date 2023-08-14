from delta.tables import *
from pyspark.sql.functions import *

from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode, Diagnostic, SeverityLevelEnum, \
    SubstituteDisabled, PostSubstituteDisabled
from prophecy.cb.server.base.DatasetBuilderBase import DatasetSpec, DatasetProperties, Component
from prophecy.cb.ui.UISpecUtil import validateExpTable
from prophecy.cb.ui.UISpecUtil import validateSColumn
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.NumberUtils import parseInt
from prophecy.cb.util.StringUtils import isBlank
from prophecy.cb.server.base import WorkflowContext

class catalogTable(DatasetSpec):
    name: str = "catalogTable"
    datasetType: str = "Database"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/source-target/catalog-table/delta"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class CatalogTableProperties(DatasetProperties):
        schema: Optional[StructType] = None
        description: Optional[str] = ""
        path: str = ""
        tableName: str = ""
        useExternalFilePath: Optional[bool] = False
        externalFilePath: Optional[str] = ""
        timestampAsOf: Optional[str] = None
        versionAsOf: Optional[str] = None
        writeMode: Optional[str] = "error"
        partitionColumns: Optional[List[str]] = None
        replaceWhere: Optional[str] = None
        insertInto: Optional[bool] = None
        overwriteSchema: Optional[bool] = None
        mergeSchema: Optional[bool] = None
        optimizeWrite: Optional[bool] = None
        mergeSourceAlias: Optional[str] = "source"
        mergeTargetAlias: Optional[str] = "target"
        mergeCondition: Optional[SColumn] = None
        activeTab: Optional[str] = "whenMatched"
        matchedAction: Optional[str] = "update"
        matchedActionDelete: Optional[str] = "ignore"
        notMatchedAction: Optional[str] = "insert"
        matchedCondition: Optional[SColumn] = None
        matchedConditionDelete: Optional[SColumn] = None
        notMatchedCondition: Optional[SColumn] = None
        matchedTable: Optional[List[SColumnExpression]] = field(default_factory=list)
        notMatchedTable: Optional[List[SColumnExpression]] = field(default_factory=list)
        keyColumns: Optional[List[str]] = field(default_factory=list)
        historicColumns: Optional[List[str]] = field(default_factory=list)
        fromTimeCol: Optional[str] = None
        toTimeCol: Optional[str] = None
        minFlagCol: Optional[str] = None
        maxFlagCol: Optional[str] = None
        flagValue: Optional[str] = "integer"

        fileFormat: Optional[str] = "parquet"
        provider: Optional[str] = "delta"
        filterQuery: Optional[str] = ""
        isCatalogEnabled: Optional[bool] = None
        catalog: Optional[str] = None

    def sourceDialog(self) -> DatasetDialog:
        fieldPicker = FieldPicker(height=("100%")) \
            .addField(
            TextArea("Description", 2, placeholder="Dataset description..."),
            "description",
            True
        ).addField(
            SelectBox("Provider")
                .addOption("delta", "delta")
                .addOption("hive", "hive"),
            "provider",
            True
        )

        return DatasetDialog("catalogTable") \
            .addSection(
            "LOCATION",
            ColumnsLayout()
                .addColumn(
                StackLayout(direction=("vertical"), gap=("1rem"))
                    .addElement(
                    CatalogTableDB("").bindProperty("path").bindTableProperty("tableName").bindCatalogProperty(
                        "catalog").bindIsCatalogEnabledProperty("isCatalogEnabled"))
            )
        ) \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap=("1rem"), height=("100%"))
                .addColumn(
                StackLayout()
                    .addElement(
                    Condition()
                        .ifEqual(
                        PropExpr("component.properties.provider"),
                        StringExpr("delta"),
                    )
                        .then(
                        StackLayout(height=("100%")).addElement(
                            StackItem(grow=(1)).addElement(
                                fieldPicker
                                    .addField(TextBox("Read timestamp").bindPlaceholder(""), "timestampAsOf")
                                    .addField(TextBox("Read version").bindPlaceholder(""), "versionAsOf")
                            ).addElement(
                                ScrollBox()
                                    .addElement(TitleElement(title="Filter Predicate"))
                                    .addElement(Editor(height=("30bh")).bindProperty("filterQuery"))
                            )
                        )
                    ).otherwise(
                        StackLayout(height=("100%")).addElement(
                            StackItem(grow=(1)).addElement(
                                fieldPicker
                            ).addElement(
                                ScrollBox()
                                    .addElement(TitleElement(title="Filter Predicate"))
                                    .addElement(Editor(height=("40bh")).bindProperty("filterQuery"))
                            )
                        )
                    )
                ),
                "auto")
                .addColumn(SchemaTable("").isReadOnly().bindProperty("schema"), "5fr")
        ) \
            .addSection(
            "PREVIEW",
            PreviewTable("").bindProperty("schema")
        )

    def targetDialog(self) -> DatasetDialog:
        matchedTable = ExpTable("Expressions").bindProperty("matchedTable")
        notMatchedTable = ExpTable("Expressions").bindProperty("notMatchedTable")
        mergeView = StackLayout() \
            .addElement(TitleElement("Merge Condition")) \
            .addElement(
            ColumnsLayout(gap=("1rem"))
                .addColumn(
                TextBox("Source Alias: for the new data coming in")
                    .bindPlaceholder("source alias")
                    .bindProperty("mergeSourceAlias")
            )
                .addColumn(
                TextBox("Target Alias: for the new existing data")
                    .bindPlaceholder("target alias")
                    .bindProperty("mergeTargetAlias")
            )
        ) \
            .addElement(NativeText("Merge condition: checks if you need to merge this row")) \
            .addElement(
            Editor(height=("40bh"))
                .makeFieldOptional().withSchemaSuggestions()
                .bindProperty("mergeCondition")
        ) \
            .addElement(TitleElement("Custom Clauses")) \
            .addElement(
            StackLayout(height=("100bh"))
                .addElement(
                Tabs()
                    .bindProperty("activeTab")
                    .addTabPane(
                    TabPane("When Matched Update", "whenMatched")
                        .addElement(
                        ColumnsLayout(gap=("1rem"))
                            .addColumn(
                            StackLayout(height=("100%"))
                                .addElement(
                                SelectBox("Action")
                                    .addOption("update", "update")
                                    .addOption("ignore", "ignore")
                                    .bindProperty("matchedAction")
                            )
                                .addElement(
                                Condition()
                                    .ifNotEqual(
                                    PropExpr("component.properties.matchedAction"),
                                    StringExpr("ignore")
                                )
                                    .then(
                                    ColumnsLayout(gap=("1rem"))
                                        .addColumn(
                                        StackLayout()
                                            .addElement(
                                            NativeText("Only when the additional condition is true")
                                        )
                                            .addElement(
                                            Editor(height=("40bh"))
                                                .makeFieldOptional().withSchemaSuggestions()
                                                .bindProperty("matchedCondition")
                                        )
                                    )
                                        .addColumn(
                                        Condition()
                                            .ifEqual(
                                            PropExpr("component.properties.matchedAction"),
                                            StringExpr("update")
                                        )
                                            .then(
                                            StackLayout(height=("100%"))
                                                .addElement(
                                                NativeText(
                                                    "Replace default update with these expressions (optional)"
                                                )
                                            )
                                                .addElement(matchedTable)
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
                    .addTabPane(
                    TabPane("When Matched Delete", "whenMatchedDelete")
                        .addElement(
                        ColumnsLayout(gap=("1rem"))
                            .addColumn(
                            StackLayout(height=("100%"))
                                .addElement(
                                SelectBox("Action")
                                    .addOption("delete", "delete")
                                    .addOption("ignore", "ignore")
                                    .bindProperty("matchedActionDelete")
                            )
                                .addElement(
                                Condition()
                                    .ifNotEqual(
                                    PropExpr("component.properties.matchedActionDelete"),
                                    StringExpr("ignore")
                                )
                                    .then(
                                    ColumnsLayout(gap=("1rem"))
                                        .addColumn(
                                        StackLayout()
                                            .addElement(
                                            NativeText("Only when the additional condition is true")
                                        )
                                            .addElement(
                                            Editor(height=("40bh"))
                                                .makeFieldOptional().withSchemaSuggestions()
                                                .bindProperty("matchedConditionDelete")
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
                    .addTabPane(
                    TabPane("When Not Matched", "notMatched")
                        .addElement(
                        ColumnsLayout(gap=("1rem"))
                            .addColumn(
                            StackLayout(height=("100%"))
                                .addElement(
                                SelectBox("Action")
                                    .addOption("insert", "insert")
                                    .addOption("ignore", "ignore")
                                    .bindProperty("notMatchedAction")
                            )
                                .addElement(
                                Condition()
                                    .ifNotEqual(
                                    PropExpr("component.properties.notMatchedAction"),
                                    StringExpr("ignore")
                                )
                                    .then(
                                    ColumnsLayout(gap=("1rem"))
                                        .addColumn(
                                        StackLayout()
                                            .addElement(
                                            NativeText("Only when the additional condition is true")
                                        )
                                            .addElement(
                                            Editor(height=("40bh"))
                                                .makeFieldOptional().withSchemaSuggestions()
                                                .bindProperty("notMatchedCondition")
                                        )
                                    )
                                        .addColumn(
                                        StackLayout(height=("100%"))
                                            .addElement(
                                            NativeText(
                                                "Replace default update with these expressions (optional)"
                                            )
                                        )
                                            .addElement(notMatchedTable)
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )

        scd2View = StackLayout() \
            .addElement(TitleElement("Merge Details")) \
            .addElement(
            StackLayout(height=("100%"))
                .addElement(
                SchemaColumnsDropdown("Key Columns")
                    .withMultipleSelection()
                    .bindSchema("schema")
                    .bindProperty("keyColumns")
                    .showErrorsFor("keyColumns")
            )
                .addElement(
                SchemaColumnsDropdown("Historic Columns")
                    .withMultipleSelection()
                    .bindSchema("schema")
                    .bindProperty("historicColumns")
                    .showErrorsFor("historicColumns")
            )
        ) \
            .addElement(TitleElement("Time Columns")) \
            .addElement(
            StackLayout(height=("100%"))
                .addElement(
                SchemaColumnsDropdown("From time column")
                    .bindSchema("schema")
                    .bindProperty("fromTimeCol")
                    .showErrorsFor("fromTimeCol")
            )
                .addElement(
                SchemaColumnsDropdown("To time column")
                    .bindSchema("schema")
                    .bindProperty("toTimeCol")
                    .showErrorsFor("toTimeCol")
            )
        ) \
            .addElement(TitleElement("Flags")) \
            .addElement(
            StackLayout(height=("100%"))
                .addElement(
                ColumnsLayout(gap=("1rem"))
                    .addColumn(
                    TextBox("Name of the column used as min/old-value flag")
                        .bindPlaceholder("Enter the name of the column")
                        .bindProperty("minFlagCol")
                )
                    .addColumn(
                    TextBox("Name of the column used as max/latest flag")
                        .bindPlaceholder("Enter the name of the column")
                        .bindProperty("maxFlagCol")
                )
            )
                .addElement(
                SelectBox("Flag values")
                    .addOption("0/1", "integer")
                    .addOption("true/false", "boolean")
                    .bindProperty("flagValue")
            )
        )

        fieldPicker = FieldPicker(height=("100%")) \
            .addField(
            TextArea("Description", 2, placeholder="Dataset description..."),
            "description",
            True
        ).addField(
            SelectBox("Provider")
                .addOption("delta", "delta")
                .addOption("hive", "hive"),
            "provider",
            True
        )

        deltaFieldPicker = fieldPicker.addField(
            SelectBox("Write Mode")
                .addOption("overwrite", "overwrite")
                .addOption("error", "error")
                .addOption("append", "append")
                .addOption("ignore", "ignore")
                .addOption("merge", "merge")
                .addOption("scd2 merge", "merge_scd2"),
            "writeMode", True
        ).addField(Checkbox("Use insert into"), "insertInto") \
            .addField(Checkbox("Overwrite table schema"), "overwriteSchema") \
            .addField(Checkbox("Merge dataframe schema into table schema"), "mergeSchema") \
            .addField(
            SchemaColumnsDropdown("Partition Columns")
                .withMultipleSelection()
                .bindSchema("schema")
                .showErrorsFor("partitionColumns"),
            "partitionColumns"
        ) \
            .addField(TextBox("Overwrite partition predicate").bindPlaceholder(""),
                      "replaceWhere") \
            .addField(Checkbox("Optimize write"), "optimizeWrite")

        hiveFieldPicker = fieldPicker.addField(
            SelectBox("Write Mode")
                .addOption("overwrite", "overwrite")
                .addOption("error", "error")
                .addOption("append", "append")
                .addOption("ignore", "ignore"),
            "writeMode", True
        ).addField(
            SelectBox("File Format")
                .addOption("sequencefile", "sequencefile")
                .addOption("rcfile", "rcfile")
                .addOption("orc", "orc")
                .addOption("parquet", "parquet")
                .addOption("textfile", "textfile")
                .addOption("avro", "avro"),
            "fileFormat"
        ).addField(Checkbox("Use insert into"), "insertInto") \
            .addField(
            SchemaColumnsDropdown("Partition Columns")
                .withMultipleSelection()
                .bindSchema("schema")
                .showErrorsFor("partitionColumns"),
            "partitionColumns"
        )

        return DatasetDialog("catalogTable") \
            .addSection(
            "LOCATION",
            StackLayout()
                .addElement(CatalogTableDB("").bindProperty("path").bindTableProperty("tableName").bindCatalogProperty(
                "catalog").bindIsCatalogEnabledProperty("isCatalogEnabled"))
                .addElement(Checkbox("Use File Path").bindProperty("useExternalFilePath"))
                .addElement(
                Condition()
                    .ifEqual(
                    PropExpr("component.properties.useExternalFilePath"),
                    BooleanExpr(True),
                )
                    .then(
                    TextBox(
                        "File location", placeholder="dbfs:/FileStore/delta/tableName"
                    ).bindProperty("externalFilePath")
                )
            )
        ) \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap=("1rem"), height=("100%"))
                .addColumn(
                StackLayout().addElement(
                    StackLayout(height=("100%")).addElement(
                        StackItem(grow=(1)).addElement(
                            Condition()
                                .ifEqual(
                                PropExpr("component.properties.provider"),
                                StringExpr("delta"),
                            ).then(deltaFieldPicker)
                                .otherwise(hiveFieldPicker)
                        )
                    )
                ),
                "auto"
            )
                .addColumn(
                Condition()
                    .ifEqual(PropExpr("component.properties.provider"), StringExpr("hive"), )
                    .then(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"))
                    .otherwise(
                    Condition()
                        .ifEqual(PropExpr("component.properties.writeMode"), StringExpr("merge"))
                        .then(mergeView)
                        .otherwise(
                        Condition()
                            .ifEqual(PropExpr("component.properties.writeMode"), StringExpr("merge_scd2"))
                            .then(scd2View)
                            .otherwise(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"))
                    )),
                "5fr"
            )
        )

    def validate(self, context: WorkflowContext, component: Component) -> list:
        diagnostics = super(catalogTable, self).validate(context, component)
        import re
        NAME_PATTERN = re.compile(r"^[\w]+$")
        CONFIG_NAME_PATTERN = re.compile(r"^\$(.*)$")

        props = component.properties

        if component.properties.isCatalogEnabled and (len(component.properties.catalog) == 0):
            diagnostics.append(
                Diagnostic("properties.catalog", "Catalog Name cannot be empty [Location]", SeverityLevelEnum.Error))

        if len(component.properties.path) == 0:
            diagnostics.append(
                Diagnostic("properties.path", "Database Name cannot be empty [Location]", SeverityLevelEnum.Error))
        # else:
        #     if ((not CONFIG_NAME_PATTERN.match(component.properties.path)) and (
        #             not NAME_PATTERN.match(component.properties.path))):
        #         diagnostics.append(Diagnostic(
        #             "properties.newDataset.path",
        #             f"{component.properties.path} is not a valid name for databases. Valid names only contain alphabet characters, numbers and _. [Location]",
        #             SeverityLevelEnum.Error
        #         ))

        if len(component.properties.tableName) == 0:
            diagnostics.append(
                Diagnostic("properties.tableName", "Table Name cannot be empty [Location]", SeverityLevelEnum.Error))

        # else:
        #     if ((not CONFIG_NAME_PATTERN.match(component.properties.tableName)) and (
        #             not NAME_PATTERN.match(component.properties.tableName))):
        #         diagnostics.append(Diagnostic(
        #             "properties.newDataset.tableName",
        #             f"{component.properties.tableName} is not a valid name for tables. Valid names only contain alphabet characters, numbers and _. [Location]",
        #             SeverityLevelEnum.Error
        #         ))
        if component.properties.useExternalFilePath and isBlank(component.properties.externalFilePath):
            diagnostics.append(
                Diagnostic(f"properties.useExternalFilePath", "File Location cannot be empty", SeverityLevelEnum.Error))

        if not isBlank(props.versionAsOf) and parseInt(props.versionAsOf) is None:
            diagnostics.append(
                Diagnostic("properties.versionAsOF", "Invalid version [Properties]", SeverityLevelEnum.Error))

        if props.provider == "hive" and props.writeMode in ["merge", "merge_scd2"]:
            diagnostics.append(
                Diagnostic("properties.writeMode", "Please select valid write mode from dropdown",
                           SeverityLevelEnum.Error))

        # validation for when deltaMerge selected
        if props.writeMode is not None and props.writeMode == "merge":
            # validate merge condition
            if props.mergeCondition is None:
                diagnostics.append(
                    Diagnostic("properties.mergeCondition", "Merge condition can not be empty [Properties]",
                               SeverityLevelEnum.Error))
            elif not props.mergeCondition.isExpressionPresent():
                diagnostics.append(
                    Diagnostic("properties.mergeCondition", "Merge condition can not be empty [Properties]",
                               SeverityLevelEnum.Error))
            else:
                diagnostics.extend(validateSColumn(props.mergeCondition, "condition", component))

            # validate source and target alias presence
            if isBlank(props.mergeSourceAlias):
                diagnostics.append(
                    Diagnostic("properties.mergeSourceAlias", "Source Alias can not be empty [Properties]",
                               SeverityLevelEnum.Error))

            if isBlank(props.mergeTargetAlias):
                diagnostics.append(Diagnostic(
                    "properties.mergeTargetAlias",
                    "Target Alias can not be empty [Properties]",
                    SeverityLevelEnum.Error
                ))

            # Both matched and notmatched action cannot be "ignore" simultaneously
            if props.matchedAction == "ignore" and props.matchedActionDelete == "ignore" and props.notMatchedAction == "ignore":
                diagnostics.append(Diagnostic(
                    "properties.matchedAction",
                    """At least one custom clauses ("When matched Update" or "When matched Delete" or "When not matched") has to be enabled. [Properties]""",
                    SeverityLevelEnum.Error
                ))

            # validate matched action update tab: exptable and matchedCondition
            if props.matchedAction == "update":
                if props.matchedCondition is not None:
                    if props.matchedCondition.isExpressionPresent():
                        diagnostics.extend(validateSColumn(props.matchedCondition, "condition", component))

            if props.matchedActionDelete == "delete":
                if props.matchedConditionDelete is not None:
                    if props.matchedConditionDelete.isExpressionPresent():
                        diagnostics.extend(validateSColumn(props.matchedConditionDelete, "condition", component))

            if props.matchedAction == "update":
                d1 = [x.appendMessage("[When Matched Update]") for x in
                      validateExpTable(props.matchedTable, "matchedTable", component)]
                diagnostics.extend(d1)

            # validate not matched action Tab: exptable and notmatchedCondition
            if props.notMatchedAction == "insert":
                if props.notMatchedCondition is not None:
                    if props.notMatchedCondition.isExpressionPresent():
                        diagnostics.extend(validateSColumn(props.notMatchedCondition, "condition", component))

                d2 = [x.appendMessage("[When Not Matched]") for x in
                      validateExpTable(props.notMatchedTable, "notMatchedTable", component)]
                diagnostics.extend(d2)

        # validation for when deltaSCD2 selected
        if props.writeMode == "merge_scd2":
            if len(props.keyColumns) == 0:
                diagnostics.append(
                    Diagnostic("properties.keyColumns", "Select at least one key column.", SeverityLevelEnum.Error))
            if len(props.historicColumns) == 0:
                diagnostics.append(Diagnostic("properties.historicColumns", "Historic Columns cannot be empty.",
                                              SeverityLevelEnum.Error))
            if isBlank(props.fromTimeCol):
                diagnostics.append(
                    Diagnostic("properties.fromTimeCol", "fromTimeCol cannot be empty", SeverityLevelEnum.Error))
            if isBlank(props.toTimeCol):
                diagnostics.append(
                    Diagnostic("properties.toTimeCol", "toTimeCol cannot be empty", SeverityLevelEnum.Error))
            if isBlank(props.minFlagCol):
                diagnostics.append(
                    Diagnostic("properties.minFlagCol", "minFlagCol cannot be empty", SeverityLevelEnum.Error))
            if isBlank(props.maxFlagCol):
                diagnostics.append(
                    Diagnostic("properties.maxFlagCol", "maxFlagCol cannot be empty", SeverityLevelEnum.Error))

        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[CatalogTableProperties],
                 newState: Component[CatalogTableProperties]) -> Component[
        CatalogTableProperties]:
        newProps = newState.properties

        if newProps.writeMode == "merge":
            if newProps.matchedAction == "update":
                cleanMatchedConditionUpdate, cleanMatchedTableUpdate = newProps.matchedCondition, newProps.matchedTable
            else:
                cleanMatchedConditionUpdate, cleanMatchedTableUpdate = None, []

            if newProps.matchedActionDelete == "delete":
                cleanMatchedConditionDelete = newProps.matchedConditionDelete
            else:
                cleanMatchedConditionDelete = None

            if newProps.notMatchedAction == "ignore":
                cleanNotMatchedCondition, cleanNotMatchedTable = None, []
            else:
                cleanNotMatchedCondition, cleanNotMatchedTable = newProps.notMatchedCondition, newProps.notMatchedTable

            newState.bindProperties(
                replace(newProps,
                        matchedCondition=cleanMatchedConditionUpdate,
                        matchedConditionDelete=cleanMatchedConditionDelete,
                        matchedTable=cleanMatchedTableUpdate,
                        notMatchedCondition=cleanNotMatchedCondition,
                        notMatchedTable=cleanNotMatchedTable
                        )
            )
        else:
            newState.bindProperties(
                replace(newProps,
                        mergeCondition=None,
                        matchedCondition=None,
                        matchedConditionDelete=None,
                        notMatchedCondition=None,
                        matchedTable=[],
                        notMatchedTable=[]
                        )
            )
        return newState

    class CatalogTableCode(ComponentCode):
        def __init__(self, props):
            self.props: catalogTable.CatalogTableProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            table_name = f"`{self.props.catalog}`.`{self.props.path}`.`{self.props.tableName}`" if self.props.isCatalogEnabled else f"`{self.props.path}`.`{self.props.tableName}`"
            if self.props.provider == "delta":
                if not isBlank(self.props.filterQuery):
                    if self.props.versionAsOf is not None:
                        df = spark.sql(
                            f'SELECT * FROM {table_name} VERSION AS OF {self.props.versionAsOf} WHERE {self.props.filterQuery}')
                    elif self.props.timestampAsOf is not None:
                        df = spark.sql(
                            f'SELECT * FROM {table_name} TIMESTAMP AS OF "{self.props.timestampAsOf}" WHERE {self.props.filterQuery}')
                    else:
                        df = spark.sql(
                            f'SELECT * FROM {table_name} WHERE {self.props.filterQuery}')
                elif self.props.versionAsOf is not None:
                    df = spark.sql(
                        f'SELECT * FROM {table_name} VERSION AS OF {self.props.versionAsOf}')
                elif self.props.timestampAsOf is not None:
                    df = spark.sql(
                        f'SELECT * FROM {table_name} TIMESTAMP AS OF "{self.props.timestampAsOf}"')
                else:
                    df = spark.read.table(table_name)
            elif self.props.provider == "hive":
                if not isBlank(self.props.filterQuery):
                    df = spark.sql(
                        f'SELECT * FROM {table_name} WHERE {self.props.filterQuery}')
                else:
                    df = spark.read.table(table_name)
            else:
                df = spark.read.table(table_name)
            return df

        def targetApply(self, spark: SparkSession, in0: DataFrame):

            table_name = f"`{self.props.catalog}`.`{self.props.path}`.`{self.props.tableName}`" if self.props.isCatalogEnabled else f"`{self.props.path}`.`{self.props.tableName}`"
            tableExists = spark.catalog._jcatalog.tableExists(table_name)
            if self.props.writeMode == "merge" and tableExists:
                from delta.tables import DeltaTable, DeltaMergeBuilder
                dt = DeltaTable.forName(spark, table_name) \
                    .alias(self.props.mergeTargetAlias) \
                    .merge(in0.alias(self.props.mergeSourceAlias), self.props.mergeCondition.column())

                resMatched: DeltaMergeBuilder = dt

                if self.props.matchedActionDelete == "delete":
                    if self.props.matchedConditionDelete is not None:
                        resMatched = resMatched.whenMatchedDelete(condition=self.props.matchedConditionDelete.column())
                    else:
                        resMatched = resMatched.whenMatchedDelete()

                if self.props.matchedAction == "update":
                    matched_expr = {}
                    if len(self.props.matchedTable) > 0:
                        for scol in self.props.matchedTable:
                            target_col = scol.target
                            matched_expr[target_col] = scol.expression.column()

                    if self.props.matchedCondition is not None:
                        if len(self.props.matchedTable) > 0:
                            resMatched = resMatched.whenMatchedUpdate(
                                condition=self.props.matchedCondition.column(),
                                set=matched_expr)
                        else:
                            resMatched = resMatched.whenMatchedUpdateAll(
                                condition=self.props.matchedCondition.column())
                    else:
                        if len(self.props.matchedTable) > 0:
                            resMatched = resMatched.whenMatchedUpdate(set=matched_expr)
                        else:
                            resMatched = resMatched.whenMatchedUpdateAll()

                if self.props.notMatchedAction is not None:
                    if self.props.notMatchedAction == "insert":
                        not_matched_expr = {}
                        if len(self.props.notMatchedTable) > 0:
                            for scol in self.props.notMatchedTable:
                                target_col = scol.target
                                not_matched_expr[target_col] = scol.expression.column()

                        if self.props.notMatchedCondition is not None:
                            if len(self.props.notMatchedTable) > 0:
                                resMatched = resMatched.whenNotMatchedInsert(
                                    condition=self.props.notMatchedCondition.column(),
                                    values=not_matched_expr)
                            else:
                                resMatched = resMatched.whenNotMatchedInsertAll(
                                    condition=self.props.notMatchedCondition.column())
                        else:
                            if len(self.props.notMatchedTable) > 0:
                                resMatched = resMatched.whenNotMatchedInsert(values=not_matched_expr)
                            else:
                                resMatched = resMatched.whenNotMatchedInsertAll()

                resMatched.execute()
            elif self.props.writeMode == "merge_scd2" and tableExists:
                from delta.tables import DeltaTable, DeltaMergeBuilder
                keyColumns = self.props.keyColumns
                scdHistoricColumns: PostSubstituteDisabled = self.props.historicColumns
                fromTimeColumn = self.props.fromTimeCol
                toTimeColumn = self.props.toTimeCol
                minFlagColumn = self.props.minFlagCol
                maxFlagColumn = self.props.maxFlagCol
                flagY = "1"
                flagN = "0"
                if self.props.flagValue == "boolean":
                    flagY = "true"
                    flagN = "false"

                updatesDF: SubstituteDisabled = in0.withColumn(minFlagColumn, lit(flagY)).withColumn(maxFlagColumn,
                                                                                                     lit(flagY))
                updateColumns: SubstituteDisabled = updatesDF.columns

                existingTable: SubstituteDisabled = DeltaTable.forName(spark,
                                                                       table_name)
                existingDF: SubstituteDisabled = existingTable.toDF()

                cond = None
                for scdCol in scdHistoricColumns:  # skipLargeLoopUnRolling
                    if cond is None:
                        cond = (~ (existingDF[scdCol]).eqNullSafe(updatesDF[scdCol]))
                    else:
                        cond = (cond | (~ (existingDF[scdCol]).eqNullSafe(updatesDF[scdCol])))

                rowsToUpdate = updatesDF \
                    .join(existingDF, keyColumns) \
                    .where(
                    (existingDF[maxFlagColumn] == lit(flagY)) & (
                        cond
                    )) \
                    .select(*[updatesDF[val] for val in updateColumns]) \
                    .withColumn(minFlagColumn, lit(flagN))

                stagedUpdatesDF: SubstituteDisabled = rowsToUpdate \
                    .withColumn("mergeKey", lit(None)) \
                    .union(updatesDF.withColumn("mergeKey", concat(*keyColumns)))

                updateCond = None
                for scdCol in scdHistoricColumns:  # skipLargeLoopUnRolling
                    if updateCond is None:
                        updateCond = (~ (existingDF[scdCol]).eqNullSafe(stagedUpdatesDF[scdCol]))
                    else:
                        updateCond = (updateCond | (~ (existingDF[scdCol]).eqNullSafe(stagedUpdatesDF[scdCol])))

                existingTable \
                    .alias("existingTable") \
                    .merge(
                    stagedUpdatesDF.alias("staged_updates"),
                    concat(*[existingDF[key] for key in keyColumns]) == stagedUpdatesDF["mergeKey"]
                ) \
                    .whenMatchedUpdate(
                    condition=(existingDF[maxFlagColumn] == lit(flagY)) & updateCond,
                    set={
                        maxFlagColumn: flagN,
                        toTimeColumn: ("staged_updates." + fromTimeColumn)
                    }
                ).whenNotMatchedInsertAll() \
                    .execute()
            else:
                writer = in0.write.format(self.props.provider)
                if self.props.provider == "delta" and self.props.optimizeWrite is not None:
                    writer = writer.option("optimizeWrite", self.props.optimizeWrite)
                if self.props.provider == "hive":
                    writer = writer.option("fileFormat", self.props.fileFormat)
                if self.props.provider == "delta" and self.props.mergeSchema is not None:
                    writer = writer.option("mergeSchema", self.props.mergeSchema)
                if self.props.provider == "delta" and self.props.replaceWhere is not None:
                    writer = writer.option("replaceWhere", self.props.replaceWhere)
                if self.props.provider == "delta" and self.props.overwriteSchema is not None:
                    writer = writer.option("overwriteSchema", self.props.overwriteSchema)
                if self.props.useExternalFilePath:
                    writer = writer.option("path", self.props.externalFilePath)

                if self.props.writeMode is not None:
                    if self.props.writeMode in ["merge", "merge_scd2"]:
                        writer = writer.mode("overwrite")
                    else:
                        writer = writer.mode(self.props.writeMode)

                if self.props.partitionColumns is not None and len(self.props.partitionColumns) > 0:
                    writer = writer.partitionBy(*self.props.partitionColumns)

                if self.props.insertInto:
                    writer.insertInto(table_name)
                else:
                    writer.saveAsTable(table_name)
