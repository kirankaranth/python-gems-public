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


class delta(DatasetSpec):
    name: str = "delta"
    datasetType: str = "File"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/source-target/file/delta"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class DeltaProperties(DatasetProperties):
        schema: Optional[StructType] = None
        description: Optional[str] = ""
        path: str = ""
        timestampAsOf: Optional[str] = None
        versionAsOf: Optional[str] = None
        writeMode: Optional[str] = None
        partitionColumns: Optional[List[str]] = None
        replaceWhere: Optional[str] = None
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
        matchedTable: List[SColumnExpression] = field(default_factory=list)
        notMatchedTable: List[SColumnExpression] = field(default_factory=list)
        keyColumns: List[str] = field(default_factory=list)
        historicColumns: List[str] = field(default_factory=list)
        fromTimeCol: Optional[str] = None
        toTimeCol: Optional[str] = None
        minFlagCol: Optional[str] = None
        maxFlagCol: Optional[str] = None
        flagValue: Optional[str] = "integer"

    def sourceDialog(self) -> DatasetDialog:
        return DatasetDialog("delta") \
            .addSection("LOCATION", TargetLocation("path")) \
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
                                .addField(TextBox("Read timestamp").bindPlaceholder(""), "timestampAsOf")
                                .addField(TextBox("Read version").bindPlaceholder(""), "versionAsOf")
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

        return DatasetDialog("delta") \
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
                                .addField(SelectBox("Write Mode")
                                          .addOption("overwrite", "overwrite")
                                          .addOption("error", "error")
                                          .addOption("append", "append")
                                          .addOption("ignore", "ignore")
                                          .addOption("merge", "merge")
                                          .addOption("scd2 merge", "merge_scd2"),
                                          "writeMode",
                                          True)
                                .addField(Checkbox("Overwrite table schema"), "overwriteSchema")
                                .addField(Checkbox("Merge dataframe schema into table schema"), "mergeSchema")
                                .addField(
                                SchemaColumnsDropdown("Partition Columns")
                                    .withMultipleSelection()
                                    .bindSchema("schema")
                                    .showErrorsFor("partitionColumns"),
                                "partitionColumns"
                            )
                                .addField(TextBox("Overwrite partition predicate").bindPlaceholder(""), "replaceWhere")
                                .addField(Checkbox("Optimize write"), "optimizeWrite")
                        )
                    )
                ),
                "auto"
            )
                .addColumn(
                Condition()
                    .ifEqual(PropExpr("component.properties.writeMode"), StringExpr("merge"))
                    .then(mergeView)
                    .otherwise(
                    Condition()
                        .ifEqual(PropExpr("component.properties.writeMode"), StringExpr("merge_scd2"))
                        .then(scd2View)
                        .otherwise(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"))
                ),
                "5fr"
            )
        )

    def validate(self, context: WorkflowContext, component: Component) -> list:
        diagnostics = super(delta, self).validate(context, component)
        props = component.properties
        if isBlank(props.path):
            diagnostics.append(
                Diagnostic("properties.path", "path variable cannot be empty [Location]", SeverityLevelEnum.Error))
        if isBlank(props.schema):
            pass
            # diagnostics .append( Diagnostic("properties.schema", "Schema cannot be empty [Properties]", SeverityLevelEnum.Error))

        if not isBlank(props.versionAsOf) and parseInt(props.versionAsOf) is None:
            diagnostics.append(
                Diagnostic("properties.versionAsOF", "Invalid version [Properties]", SeverityLevelEnum.Error))

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

    def onChange(self,context: WorkflowContext, oldState: Component[DeltaProperties], newState: Component[DeltaProperties]) -> Component[
        DeltaProperties]:
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

    class DeltaFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: delta.DeltaProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            reader = spark.read.format("delta")
            if self.props.versionAsOf is not None:
                reader = reader.option("versionAsOf", self.props.versionAsOf)
            if self.props.timestampAsOf is not None:
                reader = reader.option("timestampAsOf", self.props.timestampAsOf)
            return reader.load(self.props.path)

        def targetApply(self, spark: SparkSession, in0: DataFrame):
            from delta.tables import DeltaTable, DeltaMergeBuilder

            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
            directory_exists = fs.exists(spark._jvm.org.apache.hadoop.fs.Path(self.props.path))
            tableExists = DeltaTable.isDeltaTable(spark, self.props.path) and directory_exists

            if self.props.writeMode == "merge" and tableExists:
                dt = DeltaTable.forPath(spark, self.props.path) \
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

                existingTable: SubstituteDisabled = DeltaTable.forPath(spark, self.props.path)
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
                writer = in0.write.format("delta")
                if self.props.optimizeWrite is not None:
                    writer = writer.option("optimizeWrite", self.props.optimizeWrite)
                if self.props.mergeSchema is not None:
                    writer = writer.option("mergeSchema", self.props.mergeSchema)
                if self.props.replaceWhere is not None:
                    writer = writer.option("replaceWhere", self.props.replaceWhere)
                if self.props.overwriteSchema is not None:
                    writer = writer.option("overwriteSchema", self.props.overwriteSchema)

                if self.props.writeMode is not None:
                    if self.props.writeMode in ["merge", "merge_scd2"]:
                        writer = writer.mode("overwrite")
                    else:
                        writer = writer.mode(self.props.writeMode)
                if self.props.partitionColumns is not None and len(self.props.partitionColumns) > 0:
                    writer = writer.partitionBy(*self.props.partitionColumns)
                writer.save(self.props.path)
