from pyspark.sql import *

from prophecy.cb.server.base.ComponentBuilderBase import *
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.StringUtils import isBlank
from prophecy.cb.server.base import WorkflowContext

class FileOperation(ComponentSpec):
    name: str = "FileOperation"
    category: str = "Custom"
    gemDescription: str = "Helps perform file operations like copy and move on different file systems"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/custom/file-operations"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class FileOperationProperties(ComponentProperties):
        operation: str = "copy"
        source: str = ""
        destination: str = ""
        filesystem: str = "dbfs"
        recurse: bool = False
        fileRegex: Optional[str] = None
        ignoreEmptyFiles: Optional[bool] = False

    def dialog(self) -> Dialog:
        fsSelectBox = (SelectBox("File System")
                       .addOption("Local", "local")
                       .addOption("DBFS", "dbfs")
                       .addOption("S3 - Boto3", "s3")
                       .bindProperty("filesystem"))

        selectBox = Condition() \
            .ifEqual(PropExpr("component.properties.filesystem"), StringExpr("s3")) \
            .then(SelectBox("Operation")
                  .addOption("Copy", "copy")
                  .addOption("Move", "move")
                  .addOption("Sync", "sync")
                  .bindProperty("operation")) \
            .otherwise(SelectBox("Operation")
                       .addOption("Copy", "copy")
                       .addOption("Move", "move")
                       .bindProperty("operation"))
        return Dialog("FileOperation") \
            .addElement(ColumnsLayout(gap="1rem", height="100%")
                        .addColumn(StackLayout(height="100%")
                                   .addElement(fsSelectBox)
                                   .addElement(ColumnsLayout(gap="1rem", alignY="start")
                                               .addColumn(selectBox)
                                               .addColumn(Condition()
                                                          .ifEqual(PropExpr("component.properties.filesystem"),
                                                                   StringExpr("s3"))
                                                          .then(
            TextBox("Filename Regex", placeholder="Eg: .*stderr.*\.txt").bindProperty("fileRegex"))
                                                          .otherwise(Checkbox("Recurse").bindProperty("recurse")))
                                               .addColumn(Condition()
                                                          .ifEqual(PropExpr("component.properties.filesystem"),
                                                                   StringExpr("s3"))
                                                          .then(Checkbox("Ignore empty files")
                                                                .bindProperty("ignoreEmptyFiles")))
                                               )
                                   .addElement(
            Condition()
            .ifEqual(PropExpr("component.properties.filesystem"),
                     StringExpr("local"))
            .then(TextBox("Source Path", placeholder="Eg: /dbfs/Prophecy/test.csv")
                  .bindProperty("source"))
            .otherwise(
                Condition()
                .ifEqual(PropExpr("component.properties.filesystem"),
                         StringExpr("s3"))
                .then(
                    TextBox("Source Path", placeholder="Eg: s3://Prophecy/test.csv")
                    .bindProperty("source"))
                .otherwise(
                    TextBox("Source Path", placeholder="Eg: dbfs:/Prophecy/test.csv")
                    .bindProperty("source"))
            )
        )
                                   .addElement(Condition()
                                               .ifEqual(PropExpr("component.properties.filesystem"),
                                                        StringExpr("local"))
                                               .then(TextBox("Destination Path",
                                                             placeholder="Eg: /dbfs/Prophecy/destination/test.csv")
                                                     .bindProperty("destination"))
                                               .otherwise(Condition()
                                                          .ifEqual(PropExpr("component.properties.filesystem"),
                                                                   StringExpr("s3"))
                                                          .then(TextBox("Destination Path",
                                                                        placeholder="Eg: s3://Prophecy/destination/test.csv")
                                                                .bindProperty("destination"))
                                                          .otherwise(TextBox("Destination Path",
                                                                             placeholder="Eg: dbfs:/Prophecy/destination/test.csv")
                                                                     .bindProperty("destination")))),
                                   "2fr")
                        )

    def validate(self, context: WorkflowContext, component: Component[FileOperationProperties]) -> List[Diagnostic]:
        diagnostics = []
        if isBlank(component.properties.source):
            diagnostics.append(
                Diagnostic("properties.source", "Source path has to be specified",
                           SeverityLevelEnum.Error))
        if isBlank(component.properties.destination):
            diagnostics.append(
                Diagnostic("properties.destination", "Destination path has to be specified",
                           SeverityLevelEnum.Error))
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[FileOperationProperties], newState: Component[FileOperationProperties]) -> \
            Component[FileOperationProperties]:
        return newState

    class FileOperationCode(ComponentCode):
        def __init__(self, newProps):
            self.props: FileOperation.FileOperationProperties = newProps

        def apply(self, spark: SparkSession):
            source = self.props.source
            dest = self.props.destination
            if not 'SColumnExpression' in locals():
                if self.props.filesystem == "local":
                    import os
                    import shutil
                    if self.props.operation == "move":
                        if self.props.recurse:
                            shutil.copytree(source, dest, copy_function=shutil.copy2, dirs_exist_ok=True)
                            shutil.rmtree(source)
                            os.makedirs(source)
                        else:
                            shutil.copy2(source, dest)
                            shutil.rmtree(source)
                    if self.props.operation == "copy":
                        if self.props.recurse:
                            shutil.copytree(source, dest, copy_function=shutil.copy2, dirs_exist_ok=True)
                        else:
                            shutil.copy2(source, dest)
                elif self.props.filesystem == "dbfs":
                    from pyspark.dbutils import DBUtils
                    dbutils = DBUtils(spark)
                    if self.props.operation == "move":
                        dbutils.fs.mv(source, dest, recurse=self.props.recurse)
                    if self.props.operation == "copy":
                        dbutils.fs.cp(source, dest, recurse=self.props.recurse)
                elif self.props.filesystem == "s3":
                    import re
                    import boto3
                    from urllib.parse import urlparse
                    from botocore.exceptions import ClientError

                    mode = self.props.operation
                    fileRegex = self.props.fileRegex
                    ignoreEmptyFiles = self.props.ignoreEmptyFiles
                    src_url: SubstituteDisabled = urlparse(self.props.source)
                    dest_url: SubstituteDisabled = urlparse(self.props.destination)
                    src_bucket = src_url.netloc
                    src_prefix = src_url.path.lstrip('/')
                    dest_bucket = dest_url.netloc
                    dest_prefix = dest_url.path.lstrip('/')

                    s3 = boto3.client("s3")
                    for obj in s3.list_objects_v2(Bucket=src_bucket, Prefix=src_url.path.lstrip('/'))['Contents']:
                        new_dest_prefix = re.sub(src_prefix, dest_prefix, obj['Key'], 1)
                        src_details = s3.head_object(Bucket=src_bucket, Key=obj['Key'])
                        if mode in ['copy', 'move', 'sync'] and not obj['Key'].endswith("/"):
                            if mode == 'sync':
                                try:
                                    dest_details = s3.head_object(Bucket=dest_bucket, Key=new_dest_prefix)
                                except ClientError as e:
                                    if e.response['Error']['Code'] == "404":
                                        dest_details = None
                                    else:
                                        raise e
                            if (bool(ignoreEmptyFiles) and src_details['ContentLength'] == 0) or (
                                    bool(fileRegex) and fileRegex != "" and not bool(
                                    re.compile(fileRegex).match(obj['Key']))) or (
                                    mode == "sync" and bool(dest_details) and src_details['LastModified'] <=
                                    dest_details['LastModified']):
                                continue
                            s3.copy({'Bucket': src_bucket, 'Key': obj['Key']}, dest_bucket, new_dest_prefix)
                            if mode == "move":
                                s3.delete_object(Bucket=src_bucket, Key=obj['Key'])
