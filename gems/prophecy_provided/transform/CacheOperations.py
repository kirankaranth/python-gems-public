from pyspark.sql import *

from prophecy.cb.server.base.ComponentBuilderBase import *
from prophecy.cb.ui.uispec import *
from prophecy.cb.server.base import WorkflowContext

class CacheOperations(ComponentSpec):
    name: str = "CacheOperations"
    category: str = "Custom"
    gemDescription: str = "Allows user to cache/persist/unpersist"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class CacheOperationsProperties(ComponentProperties):
        action: str = "cache"
        eager: str = "eagerTrue"
        persistLevel: str = "MEMORY_AND_DISK"

    def dialog(self) -> Dialog:
        selectBox = (RadioGroup("Action")
                     .addOption("Persist", "persist",
                                description=(
                                        "It is an optimization mechanism to store the intermediate computation of a Spark " +
                                        "DataFrame so they can be reused in subsequent actions."))
                     .addOption("Cache", "cache",
                                description="Same as persist operation with storage level set to MEMORY_AND_DISK"
                                )
                     .addOption("Unpersist", "unpersist",
                                description=(
                                    "It will unpersist the dataframe from memory/disk."))

                     .addOption("Checkpoint", "checkpoint",
                                description="Checkpoint stores your data in a reloable storage (e.g. hdfs). It is useful for" +
                                            " truncating the lineage graph of an RDD."
                                )
                     .addOption("Local Checkpoint", "localCheckpoint",
                                description="Local checkpoint stores your data in executors storage (non-reliable). It is useful for" +
                                            " truncating the lineage graph of an RDD, however, in case of node failure you will lose the data and" +
                                            " you need to recompute it"
                                )
                     .setOptionType("button")
                     .setVariant("medium")
                     .setButtonStyle("solid")
                     .bindProperty("action")
                     )

        eagerSelectBox = (
            SelectBox("Eager Flag (Checkpointing to happen immediately or when first action is encountered).")
                .addOption("True", "eagerTrue")
                .addOption("False", "eagerFalse").bindProperty("eager"))

        persistSelectBox = (SelectBox("Storage Level")
                            .addOption("MEMORY_ONLY", "MEMORY_ONLY")
                            .addOption("MEMORY_ONLY_2", "MEMORY_ONLY_2")
                            .addOption("MEMORY_AND_DISK", "MEMORY_AND_DISK")
                            .addOption("MEMORY_AND_DISK_2", "MEMORY_AND_DISK_2")
                            .addOption("DISK_ONLY", "DISK_ONLY")
                            .addOption("DISK_ONLY_2", "DISK_ONLY_2")
                            .addOption("DISK_ONLY_3", "DISK_ONLY_3")
                            .addOption("OFF_HEAP", "OFF_HEAP").bindProperty("persistLevel"))

        persistView = (Condition()
                       .ifEqual(PropExpr("component.properties.action"), StringExpr("persist"))
                       .then(persistSelectBox))

        checkPointView = (Condition()
                          .ifEqual(PropExpr("component.properties.action"), StringExpr("checkpoint"))
                          .then(eagerSelectBox))

        localCheckPointView = (Condition()
                               .ifEqual(PropExpr("component.properties.action"),
                                        StringExpr("localCheckpoint"))
                               .then(eagerSelectBox))

        return Dialog("CacheOperations").addElement(
            ColumnsLayout(gap=("1rem"), height=("100%"))
                .addColumn(PortSchemaTabs().importSchema())
                .addColumn(
                StackLayout()
                    .addElement(selectBox)
                    .addElement(persistView)
                    .addElement(checkPointView)
                    .addElement(localCheckPointView),
                "2fr"
            )
        )

    def validate(self, context: WorkflowContext, component: Component[CacheOperationsProperties]) -> List[Diagnostic]:
        diagnostics = []

        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[CacheOperationsProperties],
                 newState: Component[CacheOperationsProperties]) -> \
            Component[
                CacheOperationsProperties]:
        return newState

    class RepartitionCode(ComponentCode):
        def __init__(self, newProps):
            self.props: CacheOperations.CacheOperationsProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:

            if self.props.action == "cache":
                return in0.select('*').cache()
            elif self.props.action == "persist":
                from pyspark import StorageLevel
                return in0.select('*').persist(eval(f"StorageLevel.{self.props.persistLevel}"))
            elif self.props.action == "unpersist":
                return in0.select('*').unpersist()
            elif self.props.action == "localCheckpoint":
                if not ("SColumnExpression" in locals()):
                    if self.props.eager == "eagerFalse":
                        return in0.localCheckpoint(eager=False)
                    else:
                        return in0.localCheckpoint()
                else:
                    return in0
            elif self.props.action == "checkpoint":
                if not ("SColumnExpression" in locals()):
                    if self.props.eager == "eagerFalse":
                        return in0.checkpoint(eager=False)
                    else:
                        return in0.checkpoint()
                else:
                    return in0
            else:
                return in0
