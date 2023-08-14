from pyspark.sql import *

from prophecy.cb.server.base.ComponentBuilderBase import *
from prophecy.cb.ui.UISpecUtil import validateSColumn
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.StringUtils import isBlank
from prophecy.cb.server.base import WorkflowContext

@dataclass(frozen=True)
class StringColName:
    colName: str


class DeltaTableOperations(ComponentSpec):
    name: str = "DeltaTableOperations"
    category: str = "Custom"
    gemDescription: str = "Perform additional Delta Table Operations"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/custom/delta-ops"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class DeltaTableOperationsProperties(ComponentProperties):
        database: Optional[str] = ""
        tableName: Optional[str] = ""
        useExternalFilePath: Optional[bool] = False
        path: Optional[str] = ""
        action: Optional[str] = None
        vaccumRetainNumHours: Optional[str] = None
        useOptimiseWhere: Optional[bool] = False
        useOptimiseZOrder: Optional[bool] = False
        optimiseWhere: Optional[str] = None
        optimiseZOrderColumns: List[StringColName] = field(default_factory=list)
        restoreVia: Optional[str] = None
        restoreValue: Optional[str] = None
        deleteCondition: SColumn = SColumn("lit(True)")
        updateCondition: Optional[SColumn] = None
        updateTableExpressions: Optional[List[SColumnExpression]] = field(default_factory=list)

    def dialog(self) -> Dialog:
        selectBox = (
            SelectBox("Action")
                .addOption("Register table in catalog", "registerTableInCatalog")
                .addOption("Vacuum table", "vaccumTable")
                .addOption("Optimise table", "optimiseTable")
                .addOption("Restore table", "restoreTable")
                .addOption("Update table", "updateTable")
                .addOption("Delete from table", "deleteFromTable")
                .addOption("Drop table", "dropTable")
                .addOption("FSCK Repair table", "fsckRepairTable")
                .bindProperty("action")
        )

        registerTableInCatalogDialog = (
            Condition()
                .ifEqual(
                PropExpr("component.properties.action"),
                StringExpr("registerTableInCatalog"),
            )
                .then(
                NativeText(
                    "This will register the data at mentioned file path as a table in catalog."
                )
            )
        )

        vaccumTableDialog = (
            Condition()
                .ifEqual(
                PropExpr("component.properties.action"),
                StringExpr("vaccumTable"),
            )
                .then(
                StackLayout()
                    .addElement(
                    NativeText(
                        "Recursively vacuum directories associated with the Delta table. VACUUM removes all files from the"
                        + " table directory that are not managed by Delta, as well as data files that are no longer in the"
                        + " latest state of the transaction log for the table and are older than a retention threshold. The"
                        + " default threshold is 7 days."
                    )
                )
                    .addElement(
                    TextBox(
                        "Retention Hours (Optional)", placeholder="100"
                    ).bindProperty("vaccumRetainNumHours")
                )
            )
        )

        updateTable = ExpTable("Update Expressions").bindProperty("updateTableExpressions")
        updateTableDialog = (
            Condition()
                .ifEqual(
                PropExpr("component.properties.action"),
                StringExpr("updateTable"),
            )
                .then(
                ColumnsLayout(gap=("1rem"))
                    .addColumn(
                    StackLayout()
                        .addElement(
                        NativeText("Update condition (where clause)")
                    )
                        .addElement(
                        Editor(height=("40bh"))
                            .makeFieldOptional()
                            .bindProperty("updateCondition")
                    )
                )
                    .addColumn(
                    StackLayout(height=("100%"))
                        .addElement(
                        NativeText(
                            "Replace default update with these expressions (optional)"
                        )
                    )
                        .addElement(updateTable)

                )
            )
        )

        deleteFromTableDialog = (
            Condition()
                .ifEqual(
                PropExpr("component.properties.action"),
                StringExpr("deleteFromTable"),
            )
                .then(
                StackLayout()
                    .addElement(
                    NativeText(
                        "Delete removes the data from the latest version of the Delta table as per the condition" +
                        " specified below. Please note that delete does not remove it from the physical storage" +
                        " until the older versions are explicitly vacuumed."
                    )
                )
                    .addElement(
                    Editor(height=("40bh")).bindProperty("deleteCondition.expression")
                )
            )
        )

        restoreTableDialog = (
            Condition()
                .ifEqual(
                PropExpr("component.properties.action"),
                StringExpr("restoreTable"),
            )
                .then(
                StackItem(grow=(1))
                    .addElement(
                    NativeText(
                        "Restores a Delta table to an earlier state. Restoring to an earlier version number or a"
                        + " timestamp is supported."
                    )
                )
                    .addElement(
                    SelectBox("Restore Via")
                        .addOption("Timestamp", "restoreViaTimestamp")
                        .addOption("Version", "restoreViaVersion")
                        .bindProperty("restoreVia")
                )
                    .addElement(
                    TextBox("Value").bindPlaceholder("").bindProperty("restoreValue")
                )
            )
        )

        optimiseTableDialog = (
            Condition()
                .ifEqual(
                PropExpr("component.properties.action"),
                StringExpr("optimiseTable"),
            )
                .then(
                StackLayout()
                    .addElement(
                    NativeText(
                        "Optimizes the layout of Delta Lake data. Optionally optimize a subset of data or colocate"
                        + " data by column. If colocation is not specified, bin-packing optimization is performed by default. "
                    )
                )
                    .addElement(
                    ColumnsLayout()
                        .addColumn(
                        Checkbox("Use where clause").bindProperty("useOptimiseWhere")
                    )
                        .addColumn(Checkbox("Use ZOrder").bindProperty("useOptimiseZOrder"))
                )
                    .addElement(
                    Condition()
                        .ifEqual(
                        PropExpr("component.properties.useOptimiseWhere"),
                        BooleanExpr(True),
                    )
                        .then(Editor(height=("20bh")).bindProperty("optimiseWhere"))
                )
                    .addElement(
                    Condition()
                        .ifEqual(
                        PropExpr("component.properties.useOptimiseZOrder"),
                        BooleanExpr(True),
                    )
                        .then(
                        BasicTable(
                            "ZOrder Columns",
                            height=("200px"),
                            columns=[
                                Column(
                                    "ZOrder Columns",
                                    "colName",
                                    (TextBox("").bindPlaceholder("col_name")),
                                )
                            ],
                            targetColumnKey="colName",
                        ).bindProperty("optimiseZOrderColumns")
                    )
                )
            )
        )

        dropTableDialog = (
            Condition()
                .ifEqual(
                PropExpr("component.properties.action"),
                StringExpr("dropTable"),
            )
                .then(
                NativeText(
                    "This will drop the table from catalog and remove the files."
                )
            )
        )

        fsckRepairDialog = (
            Condition()
                .ifEqual(
                PropExpr("component.properties.action"),
                StringExpr("fsckRepairTable"),
            )
                .then(
                NativeText(
                    "Removes the file entries from the transaction log of a Delta table that can no longer be found in "
                    + "the underlying file system. This can happen when these files have been manually deleted."
                )
            )
        )

        return Dialog("Delta Operations").addElement(
            StackLayout()
                .addElement(
                CatalogTableDB("")
                    .bindProperty("database")
                    .bindTableProperty("tableName")
            )
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
                    ).bindProperty("path")
                )
            )
                .addElement(selectBox)
                .addElement(
                registerTableInCatalogDialog.otherwise(
                    vaccumTableDialog.otherwise(
                        deleteFromTableDialog.otherwise(
                            restoreTableDialog.otherwise(
                                optimiseTableDialog.otherwise(
                                    dropTableDialog.otherwise(
                                        fsckRepairDialog.otherwise(
                                            updateTableDialog)
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )

    def validate(self, context: WorkflowContext, component: Component) -> List[Diagnostic]:
        diagnostics = []
        import re
        NAME_PATTERN = re.compile(r"^[\w]+$")
        CONFIG_NAME_PATTERN = re.compile(r"^\$(.*)$")

        if isBlank(component.properties.database) and isBlank(component.properties.path):
            diagnostics.append(
                Diagnostic("properties.database", "Both file path and database, table cannot be empty ",
                           SeverityLevelEnum.Error))

        if component.properties.action in ["registerTableInCatalog", "optimiseTable", "fsckRepairTable"]:
            if len(component.properties.database) == 0:
                diagnostics.append(
                    Diagnostic("properties.database", "Database Name cannot be empty", SeverityLevelEnum.Error))
            else:
                if ((not CONFIG_NAME_PATTERN.match(component.properties.database)) and (
                        not NAME_PATTERN.match(component.properties.database))):
                    diagnostics.append(Diagnostic(
                        "properties.database",
                        f"{component.properties.path} is not a valid name for databases. Valid names only contain alphabet characters, numbers and _.",
                        SeverityLevelEnum.Error
                    ))

            if len(component.properties.tableName) == 0:
                diagnostics.append(
                    Diagnostic("properties.tableName", "Table Name cannot be empty",
                               SeverityLevelEnum.Error))

            else:
                if ((not CONFIG_NAME_PATTERN.match(component.properties.tableName)) and (
                        not NAME_PATTERN.match(component.properties.tableName))):
                    diagnostics.append(Diagnostic(
                        "properties.tableName",
                        f"{component.properties.tableName} is not a valid name for tables. Valid names only contain alphabet characters, numbers and _.",
                        SeverityLevelEnum.Error
                    ))

        if component.properties.useExternalFilePath and isBlank(component.properties.path):
            if component.properties.action == "registerTableInCatalog":
                diagnostics.append(
                    Diagnostic(f"properties.useExternalFilePath", "File path cannot be empty", SeverityLevelEnum.Error))

        if component.properties.action == "deleteFromTable":
            diagnostics.extend(validateSColumn(component.properties.deleteCondition, "Delete condition", component))

        if component.properties.useOptimiseWhere:
            if isBlank(component.properties.optimiseWhere):
                diagnostics.append(
                    Diagnostic(f"properties.optimiseWhere", "Where condition cannot be blank", SeverityLevelEnum.Error))

        if component.properties.useOptimiseZOrder:
            if len(component.properties.optimiseZOrderColumns) == 0:
                diagnostics.append(
                    Diagnostic(f"properties.optimiseZOrderColumns", "Please provide at least one column to ZOrder by",
                               SeverityLevelEnum.Error))

        if component.properties.action == "updateTable":
            if len(component.properties.updateTableExpressions) == 0:
                diagnostics.append(
                    Diagnostic(f"properties.updateTableExpressions", "Please provide at least one expression to update",
                               SeverityLevelEnum.Error))
        return diagnostics

    def onChange(
            self,
            context: WorkflowContext,
            oldState: Component,
            newState: Component,
    ) -> Component:
        return newState

    class DeltaTableOperationsCode(ComponentCode):
        def __init__(self, newProps):
            self.props: DeltaTableOperations.DeltaTableOperationsProperties = newProps

        def apply(self, spark: SparkSession):
            if not ("SColumnExpression" in locals()):
                from delta.tables import DeltaTable

                if self.props.useExternalFilePath:
                    deltaTable = DeltaTable.forPath(
                        spark, self.props.path
                    )  # path-based tables
                else:
                    deltaTable = DeltaTable.forName(
                        spark, f"{self.props.database}.{self.props.tableName}"
                    )  # Catalog based tables

                if self.props.action == "registerTableInCatalog":
                    spark.sql(
                        "CREATE TABLE "
                        + f"{self.props.database}.{self.props.tableName}"
                        + " USING DELTA LOCATION '"
                        + self.props.path
                        + "'"
                    )

                elif self.props.action == "vaccumTable":
                    if isBlank(self.props.vaccumRetainNumHours):
                        deltaTable.vacuum()
                    else:
                        deltaTable.vacuum(
                            retentionHours=float(self.props.vaccumRetainNumHours)
                        )

                elif self.props.action == "optimiseTable":
                    zorderstr = ",".join(
                        [x.colName for x in self.props.optimiseZOrderColumns]
                    )
                    if (
                            not self.props.useOptimiseWhere
                            and not self.props.useOptimiseZOrder
                    ):
                        spark.sql(
                            f"OPTIMIZE {self.props.database}.{self.props.tableName}"
                        )
                    elif not self.props.useOptimiseZOrder:
                        spark.sql(
                            f"OPTIMIZE {self.props.database}.{self.props.tableName} WHERE {self.props.optimiseWhere}"
                        )
                    elif not self.props.useOptimiseWhere:
                        spark.sql(
                            f"""OPTIMIZE {self.props.database}.{self.props.tableName}
                        ZORDER BY {zorderstr}"""
                        )
                    else:
                        spark.sql(
                            f"""OPTIMIZE {self.props.database}.{self.props.tableName} WHERE {self.props.optimiseWhere}
                        ZORDER BY {zorderstr}"""
                        )

                elif self.props.action == "restoreTable":
                    if self.props.restoreVia == "restoreViaVersion":
                        deltaTable.restoreToVersion(int(self.props.restoreValue))
                    elif self.props.restoreVia == "restoreViaTimestamp":
                        deltaTable.restoreToTimestamp(self.props.restoreValue)

                elif self.props.action == "deleteFromTable":
                    deltaTable.delete(self.props.deleteCondition.column())

                elif self.props.action == "dropTable":
                    if not isBlank(self.props.database) and not isBlank(
                            self.props.tableName
                    ):
                        spark.sql(
                            "DROP TABLE "
                            + f"{self.props.database}.{self.props.tableName}"
                        )
                    else:
                        from pyspark.dbutils import DBUtils
                        dbutils = DBUtils(spark)
                        dbutils.fs.rm(self.props.path, True)
                elif self.props.action == "fsckRepairTable":
                    if not isBlank(self.props.database) and not isBlank(
                            self.props.tableName
                    ):
                        spark.sql(
                            "FSCK REPAIR TABLE "
                            + f"{self.props.database}.{self.props.tableName}"
                        )
                elif self.props.action == "updateTable":
                    update_expr = {}
                    if len(self.props.updateTableExpressions) > 0:
                        for scol in self.props.updateTableExpressions:
                            target_col = scol.target
                            update_expr[target_col] = scol.expression.column()

                    if self.props.updateCondition is not None:
                        deltaTable.update(
                            condition=self.props.updateCondition.column(),
                            set=update_expr)
                    else:
                        deltaTable.update(set=update_expr)
