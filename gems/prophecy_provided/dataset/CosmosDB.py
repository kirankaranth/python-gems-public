from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode, Diagnostic, SeverityLevelEnum
from prophecy.cb.server.base.DatasetBuilderBase import DatasetSpec, DatasetProperties, Component
from prophecy.cb.server.base.datatypes import SString
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.NumberUtils import parseInt
from prophecy.cb.util.StringUtils import isBlank
from prophecy.cb.server.base import WorkflowContext


class CosmosDB(DatasetSpec):
    name: str = "CosmosDB"
    datasetType: str = "Warehouse"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/source-target/warehouse/cosmos"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class CosmosDBProperties(DatasetProperties):
        schema: Optional[StructType] = None
        description: Optional[str] = ""

        # Authentication
        authType: Optional[str] = "MasterKey" # MasterKey/ServicePrincipal
        azureEnvironment: Optional[str] = "Azure" # Select Box
        accountEndpoint: Optional[str] = None
        accountKey: Optional[str] = None
        database: Optional[str] = None
        container: Optional[str] = None

        # Required for ServicePrincipal Authentication
        subscriptionId: Optional[str] = None
        tenantId: Optional[str] = None
        resourceGroupName: Optional[str] = None
        authClientId: Optional[str] = None
        authClientSecret: Optional[str] = None

        # Additional Tuning: Field Picker
        useGatewayMode: Optional[bool] = None
        forceEventualConsistency: Optional[bool] = None
        applicationName: Optional[str] = None
        preferredRegionsList: Optional[str] = None
        disableTcpConnectionEndpointRediscovery: Optional[bool] = None
        allowInvalidJsonWithDuplicateJsonProperties: Optional[bool] = None

        # Read Specific Configs
        readFromSource: Optional[str] = None
        customQuery: Optional[SString] = SString("SELECT 1")
        maxItemCount: Optional[str] = None
        maxIntegratedCacheStalenessInMS: Optional[str] = None

        # Basic Spark Write Options
        writeMode: str = "append"
        dateFormat: Optional[str] = None
        timestampFormat: Optional[str] = None

        # Write Configuration
        strategy: Optional[str] = None
        maxRetryCount: Optional[str] = None
        maxConcurrency: Optional[str] = None
        maxPendingOperations: Optional[str] = None
        bulkEnabled: Optional[bool] = None

        # Patch Configurations
        patchDefaultOperationType: Optional[str] = None # None, add, set, replace, remove, increment
        patchColumnConfigs: Optional[str] = None # Default 1000
        patchFilter: Optional[str] = None

        # Schema Inference Config
        inferSchemaEnabled: Optional[bool] = None
        inferSchemaQueryCheckbox: Optional[bool] = None
        inferSchemaQuery: Optional[SString] = SString("SELECT 1")
        inferSchemaSamplingSize: Optional[str] = None # Default 1000
        inferSchemaIncludeSystemProperties: Optional[bool] = None
        inferSchemaIncludeTimestamp: Optional[bool] = None
        inferSchemaForceNullableProperties: Optional[bool] = True

        # Serialization Config
        inclusionMode: Optional[str] = "Always" # Always/NonNull/NonEmpty
        dateTimeConversionMode: Optional[str] = "Default" # Default/AlwaysEpochMilliseconds/AlwaysEpochMillisecondsWithSystemDefaultTimezone

        # Skipping Change feed Options (only for Spark-Streaming using cosmos.oltp.changeFeed data source, which is read-only) configuration
        # Json conversion configuration
        schemaConversionMode: Optional[str] = "Relaxed" # Relaxed/Strict

        # Partitioning Strategy Config
        partitioningStrategy: Optional[str] = "Default" # Default, Custom, Restrictive or Aggressive
        partitioningTargetedCount: Optional[str] = None
        partitioningFeedRangeFilter: Optional[str] = None

        # Throughput Control Config
        throughputControlEnabled: Optional[bool] = None
        throughputControlAccountEndpoint: Optional[str] = None
        throughputControlAccountKey: Optional[str] = None
        throughputControlPreferredRegionsList: Optional[str] = None
        throughputControlDisableTcpConnectionEndpointRediscovery: Optional[bool] = None
        throughputControlUseGatewayMode: Optional[bool] = None
        throughputControlName: Optional[str] = None
        throughputControlTargetThroughput: Optional[str] = None
        throughputControlTargetThroughputThreshold: Optional[str] = None
        throughputControlGlobalControlDatabase: Optional[str] = None
        throughputControlGlobalControlContainer: Optional[str] = None
        throughputControlGlobalControlRenewIntervalInMS: Optional[str] = None
        throughputControlGlobalControlExpireIntervalInMS: Optional[str] = None
        throughputControlGlobalControlUseDedicatedContainer: Optional[bool] = None

    def sourceDialog(self) -> DatasetDialog:
        fieldPicker = FieldPicker(height=("100%")) \
            .addField(
                TextArea("Description", 2, placeholder="Dataset description..."),
                "description",
                True
            )
        def addBaseFields(fp):
            return fp.addField(Checkbox("Enable Infer Schema Options"), "inferSchemaEnabled", True) \
            .addField(TextBox("Application Name").bindPlaceholder("My App Name"), "applicationName", True) \
            .addField(
                SelectBox("Inclusion Mode")
                    .addOption("Always", "Always")\
                    .addOption("NonNull", "NonNull")\
                    .addOption("NonEmpty", "NonEmpty"),
                "inclusionMode",
                False
            ) \
            .addField(
                SelectBox("DateTime Conversion Mode")
                    .addOption("Default", "Default")\
                    .addOption("AlwaysEpochMilliseconds", "AlwaysEpochMilliseconds")\
                    .addOption("AlwaysEpochMillisecondsWithSystemDefaultTimezone", "AlwaysEpochMillisecondsWithSystemDefaultTimezone"),
                "dateTimeConversionMode"
            ) \
            .addField(
                SelectBox("Schema Conversion Mode")
                    .addOption("Relaxed", "Relaxed")\
                    .addOption("Strict", "Strict"),
                    "schemaConversionMode"
            ) \
            .addField(
                SelectBox("Partitioning Strategy")
                    .addOption("Default", "Default")\
                    .addOption("Custom", "Custom")\
                    .addOption("Restrictive", "Restrictive")\
                    .addOption("Aggressive", "Aggressive"),
                "partitioningStrategy"
            ) \
            .addField(TextBox("Preferred Regions List").bindPlaceholder("region1,region2,region3"), "preferredRegionsList") \
            .addField(Checkbox("Disable Tcp Connection Endpoint Rediscovery"), "disableTcpConnectionEndpointRediscovery") \
            .addField(Checkbox("Allow Invalid Json With Duplicate Json Properties"), "allowInvalidJsonWithDuplicateJsonProperties") \
            .addField(TextBox("Partitioning Targeted Count").bindPlaceholder("Number of Partitions to use if Partitioning strategy=Custom"), "partitioningTargetedCount") \
            .addField(TextBox("Partitioning Feed Range Filter").bindPlaceholder(""), "partitioningFeedRangeFilter") \
            .addField(Checkbox("Use Gateway Mode"), "useGatewayMode") \
            .addField(Checkbox("Force Eventual Consistency"), "forceEventualConsistency")\
            .addField(TextBox("Max Item Count").bindPlaceholder("1000"), "maxItemCount")\
            .addField(TextBox("Max Integrated Staleness in MS").bindPlaceholder("20"), "maxIntegratedCacheStalenessInMS")

        inferSchemaFieldPicker = FieldPicker(height=("40%"))\
                .addField(Checkbox("Make all Columns Nullable"), "inferSchemaForceNullableProperties", True) \
                .addField(TextBox("Record Sample Size for Schema Inference").bindPlaceholder(""), "inferSchemaSamplingSize") \
                .addField(Checkbox("Include all System Properties"), "inferSchemaIncludeSystemProperties") \
                .addField(Checkbox("Include Document Timestamp field"), "inferSchemaIncludeTimestamp") \
                .addField(Checkbox("Enable Custom Query for Inferring Schema"), "inferSchemaQueryCheckbox", True)

        baseFieldPicker = addBaseFields(fieldPicker)

        return DatasetDialog("jdbc") \
            .addSection(
            "LOCATION",
            StackLayout(direction=("vertical"), gap=("1rem"), height="100%")
                # .addElement(TitleElement(title = "Credentials"))
                .addElement(
                    RadioGroup("Authentication Type")
                        .addOption("Master Key", "MasterKey")
                        .addOption("Service Principal Based", "ServicePrincipal")
                        .bindProperty("authType")
                    )
                .addElement(
                    SelectBox("Azure Environment")
                        .addOption("Azure", "Azure")
                        .addOption("AzureChina", "AzureChina")
                        .addOption("AzureUsGovernment", "AzureUsGovernment")
                        .addOption("AzureGermany", "AzureGermany")
                    .bindProperty("azureEnvironment")
                )
                .addElement(
                    Condition()
                    .ifEqual(PropExpr("component.properties.authType"), StringExpr("MasterKey"))
                    .then(
                        StackLayout()
                        .addElement(
                        ColumnsLayout(gap=("1rem"))
                            .addColumn(TextBox("Account Endpoint").bindPlaceholder("https://example.com/").bindProperty("accountEndpoint"))
                            .addColumn(TextBox("Account Key (Use DB Secrets Config Variable)").isPassword().bindPlaceholder("Key").bindProperty("accountKey"))
                        )
                    )
                    .otherwise(
                        StackLayout()
                        .addElement(
                            ColumnsLayout(gap=("1rem"))
                                .addColumn(TextBox("Subscription Id").bindPlaceholder("subscriptionId").bindProperty("subscriptionId"))
                                .addColumn(TextBox("Tenant Id").bindProperty("tenantId"))
                        )
                        .addElement(
                            TextBox("Resource Group Name").bindProperty("resourceGroupName")
                        )
                        .addElement(
                            TextBox("Client Id").bindPlaceholder("clientId").bindProperty("authClientId")
                        )
                        .addElement(
                            TextBox("Client Secret (Use DB Secrets Config Variable)").isPassword().bindPlaceholder("Secret Key").bindProperty("authClientSecret")
                        )
                    )
                )
            .addElement(
                StackItem(grow=1).addElement(
                    StackLayout(height="100%").addElement(
                        TextBox("Database").bindPlaceholder("database").bindProperty("database")
                    ).addElement(
                        RadioGroup("Data Source")
                            .addOption("DB Table", "dbtable")
                            .addOption("SQL Query", "sqlQuery")
                            .bindProperty("readFromSource")
                    )
                    .addElement(
                        Condition()
                        .ifEqual(PropExpr("component.properties.readFromSource"), StringExpr("dbtable"))
                        .then(
                            TextBox("Container").bindPlaceholder("Container/Collection/Table Name").bindProperty("container")
                        )
                        .otherwise(
                            StackItem(grow=1).addElement(
                                 NativeText("Enter Custom Query here")
                            ).addElement(
                                Editor(height=("90%") ).withSchemaSuggestions().bindProperty("customQuery")
                            )
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
                    StackLayout(height=("100%")).addElement(
                        baseFieldPicker
                    )
                    .addElement(
                        Condition().ifEqual(PropExpr("component.properties.inferSchemaEnabled"), BooleanExpr(True))
                        .then(inferSchemaFieldPicker)
                    ).addElement(
                        Condition().ifEqual(PropExpr("component.properties.inferSchemaQueryCheckbox"), BooleanExpr(True))
                        .then(
                            StackLayout().addElement(
                                NativeText("Enter Infer Schema Query here")
                            ).addElement(
                                Editor(height = "100%")
                                    .withSchemaSuggestions()
                                    .bindProperty("inferSchemaQuery")
                            )
                        )
                    )
                ),
                "auto"
            )
                .addColumn(SchemaTable("").isReadOnly().bindProperty("schema"), "5fr")
        ) \
        .addSection(
            "THROUGHPUT CONTROL",
            ScrollBox().addElement(
                Checkbox("Enable Throughput Control Options").bindProperty("throughputControlEnabled")
            )
            .addElement(
                Condition().ifEqual(PropExpr("component.properties.throughputControlEnabled"), BooleanExpr(True))
                .then(
                    StackLayout(height=("100%"))
                    .addElement(
                        TextBox("Throughput Control Account Endpoint").bindPlaceholder("https://endpoint/").bindProperty("throughputControlAccountEndpoint")
                    ).addElement(
                    ColumnsLayout(gap=("1rem"))
                        .addColumn(TextBox("Throughput Control Account Key").bindPlaceholder("").bindProperty("throughputControlAccountKey"))
                        .addColumn(TextBox("Throughput Control Preferred Regions List").bindPlaceholder("region1,region2").bindProperty("throughputControlPreferredRegionsList"))
                    ).addElement(
                    ColumnsLayout(gap=("1rem"))
                        .addColumn(Checkbox("Disable TCP connection endpoint Rediscovery").bindProperty("throughputControlDisableTcpConnectionEndpointRediscovery"))
                        .addColumn(Checkbox("Use gateway Mode").bindProperty("throughputControlUseGatewayMode"))
                        .addColumn(Checkbox("Use Dedicated Container").bindProperty("throughputControlGlobalControlUseDedicatedContainer"))
                    ).addElement(
                    ColumnsLayout(gap=("1rem"))
                        .addColumn(TextBox("Throughput control group name").bindPlaceholder("").bindProperty("throughputControlName"))
                        .addColumn(TextBox("Renew Interval in MS").bindPlaceholder("5s").bindProperty("throughputControlGlobalControlRenewIntervalInMS"))
                        .addColumn(TextBox("Expire Interval In MS").bindPlaceholder("11s").bindProperty("throughputControlGlobalControlExpireIntervalInMS"))
                    ).addElement(
                    ColumnsLayout(gap=("1rem"))
                        .addColumn(TextBox("Throughput control group target throughput").bindPlaceholder("").bindProperty("throughputControlTargetThroughput"))
                        .addColumn(TextBox("Throughput control group target throughput threshold").bindPlaceholder("").bindProperty("throughputControlTargetThroughputThreshold"))
                    ).addElement(
                    ColumnsLayout(gap=("1rem"))
                        .addColumn(TextBox("Database which will be used for throughput global control").bindPlaceholder("").bindProperty("throughputControlGlobalControlDatabase"))
                        .addColumn(TextBox("Container which will be used for throughput global control").bindPlaceholder("").bindProperty("throughputControlGlobalControlContainer"))
                    )
                )
            )
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
                    # .addElement(TitleElement(title = "Credentials"))
                    .addElement(
                        RadioGroup("Authentication Type")
                            .addOption("Master Key", "MasterKey")
                            .addOption("Service Principle Based", "ServicePrincipal")
                            .bindProperty("authType")
                        )
                    .addElement(
                        SelectBox("Azure Environment")
                            .addOption("Azure", "Azure")
                            .addOption("AzureChina", "AzureChina")
                            .addOption("AzureUsGovernment", "AzureUsGovernment")
                            .addOption("AzureGermany", "AzureGermany")
                        .bindProperty("azureEnvironment")
                    )
                    .addElement(
                        Condition()
                        .ifEqual(PropExpr("component.properties.authType"), StringExpr("MasterKey"))
                        .then(
                            StackLayout(height=("100%"))
                            .addElement(
                            ColumnsLayout(gap=("1rem"))
                                .addColumn(TextBox("Account Endpoint").bindPlaceholder("https://example.com/").bindProperty("accountEndpoint"))
                                .addColumn(TextBox("Account Key (Use DB Secrets Config Variable)").isPassword().bindPlaceholder("Key").bindProperty("accountKey"))
                            )
                        ).otherwise(
                            StackLayout(height=("100%"))
                            .addElement(
                                ColumnsLayout(gap=("1rem"))
                                    .addColumn(TextBox("Subscription Id").bindPlaceholder("subscriptionId").bindProperty("subscriptionId"))
                                    .addColumn(TextBox("Tenant Id").bindProperty("tenantId"))
                            )
                            .addElement(
                                TextBox("Resource Group Name").bindProperty("resourceGroupName")
                            )
                            .addElement(
                                TextBox("Client Id").bindPlaceholder("clientId").bindProperty("authClientId")
                            )
                            .addElement(
                                TextBox("Client Secret (Use DB Secrets Config Variable)").isPassword().bindPlaceholder("Secret Key").bindProperty("authClientSecret")
                            )
                        )
                    )
                .addElement(
                    ColumnsLayout().addColumn(
                        TextBox("Database").bindPlaceholder("database").bindProperty("database")
                    ).addColumn(
                        TextBox("Container").bindPlaceholder("Container/Collection/Table Name").bindProperty("container")
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
                            FieldPicker(height=("100%"))
                                .addField(
                                TextArea("Description", 2, placeholder="Dataset description..."),
                                "description",
                                True
                            ).addField(
                                SelectBox("Write Mode")
                                    .addOption("error", "error")
                                    .addOption("overwrite", "overwrite")
                                    .addOption("append", "append")
                                    .addOption("ignore", "ignore"),
                                "writeMode"
                            ).addField(
                                SelectBox("Write Strategy")
                                    .addOption("ItemOverwrite", "ItemOverwrite")
                                    .addOption("ItemOverwriteIfNotModified", "ItemOverwriteIfNotModified")
                                    .addOption("ItemAppend", "ItemAppend")
                                    .addOption("ItemDelete", "ItemDelete")
                                    .addOption("ItemDeleteIfNotModified", "ItemDeleteIfNotModified")
                                    .addOption("ItemPatch", "ItemPatch"),
                                "strategy",
                                True
                            )
                            .addField(TextBox("Date Format").bindPlaceholder(""), "dateFormat")
                            .addField(TextBox("Timestamp Format").bindPlaceholder(""), "timestampFormat")
                            .addField(TextBox("Max Retry Attempts").bindPlaceholder("10"), "maxRetryCount")
                            .addField(TextBox("Max Concurrency").bindPlaceholder(""), "maxConcurrency")
                            .addField(TextBox("Max No. of Pending Bulk Operations").bindPlaceholder(""), "maxPendingOperations")
                            .addField(Checkbox("Enable Write Bulk"), "bulkEnabled", True)
                            # skipping kerberos things for now
                        )
                    ).addElement(
                        Condition().ifEqual(PropExpr("component.properties.strategy"), StringExpr("ItemPatch"))
                        .then(
                            FieldPicker(height=("100%"))
                                .addField(
                                SelectBox("Default Patch Operation Type")
                                    .addOption("None", "none")
                                    .addOption("Add", "add")
                                    .addOption("Set", "set")
                                    .addOption("Replace", "replace")
                                    .addOption("Remove", "remove")
                                    .addOption("Increment", "increment"),
                                "patchDefaultOperationType"
                                )
                                .addField(TextBox("Patch Column Configs").bindPlaceholder("col(column).op(operationType)"), "patchColumnConfigs")
                                .addField(TextBox("Patch Filter").bindPlaceholder("FROM products p WHERE p.used = false"), "patchFilter")
                            )
                        )
                    ),
                "auto"
            )
                .addColumn(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"), "5fr")
        )

    def validate(self, context: WorkflowContext, component: Component) -> list:
        diagnostics = super(CosmosDB, self).validate(context, component)

        if component.properties.authType == "ServicePrincipal":
            if isBlank(component.properties.subscriptionId):
                diagnostics.append(Diagnostic("properties.subscriptionId", "Subscription ID cannot be empty when using ServicePrincipal login [Location]",
                                              SeverityLevelEnum.Error))
            if isBlank(component.properties.tenantId):
                diagnostics.append(Diagnostic("properties.tenantId", "Tenant ID cannot be empty when using ServicePrincipal login [Location]",
                                              SeverityLevelEnum.Error))
            if isBlank(component.properties.resourceGroupName):
                diagnostics.append(Diagnostic("properties.resourceGroupName", "Resource Group Name cannot be empty when using ServicePrincipal login [Location]",
                                              SeverityLevelEnum.Error))
            if isBlank(component.properties.authClientId):
                diagnostics.append(Diagnostic("properties.authClientId", "Auth Client ID cannot be empty when using ServicePrincipal login [Location]",
                                              SeverityLevelEnum.Error))
            if isBlank(component.properties.authClientSecret):
                diagnostics.append(Diagnostic("properties.authClientSecret", "Auth Client Secret cannot be empty when using ServicePrincipal login [Location]",
                                              SeverityLevelEnum.Error))
        else:
            if isBlank(component.properties.accountEndpoint):
                diagnostics.append(Diagnostic("properties.accountEndpoint", "Account Endpoint cannot be empty [Location]",
                                              SeverityLevelEnum.Error))
            if isBlank(component.properties.accountKey):
                diagnostics.append(Diagnostic("properties.accountKey", "Account Key cannot be empty [Location]",
                                              SeverityLevelEnum.Error))
        if component.properties.readFromSource is not None:
            if component.properties.readFromSource == "dbtable":
                if isBlank(component.properties.container):
                    diagnostics.append(Diagnostic("properties.container", "Container cannot be empty if Table seleted [Location]",
                                                    SeverityLevelEnum.Error))
            else:
                if isBlank(component.properties.customQuery):
                    diagnostics.append(Diagnostic("properties.customQuery", "Query cannot be empty if Query selected [Location]",
                                                  SeverityLevelEnum.Error))
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        return newState

    class CosmosDBFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: CosmosDB.CosmosDBProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            reader = spark.read.format("cosmos.oltp")
            if self.props.authType == "MasterKey":
                reader = reader.option("spark.cosmos.accountEndpoint", self.props.accountEndpoint)
                reader = reader.option("spark.cosmos.accountKey", self.props.accountKey)
            elif self.props.authType == "ServicePrincipal":
                reader = reader.option("spark.cosmos.account.subscriptionId	", self.props.subscriptionId)
                reader = reader.option("spark.cosmos.account.tenantId", self.props.tenantId)
                reader = reader.option("spark.cosmos.account.resourceGroupName", self.props.resourceGroupName)
                reader = reader.option("spark.cosmos.auth.aad.clientId", self.props.authClientId)
                reader = reader.option("spark.cosmos.auth.aad.clientSecret", self.props.authClientSecret)
            reader = reader.option("spark.cosmos.account.azureEnvironment", self.props.azureEnvironment)
            reader = reader.option("spark.cosmos.database", self.props.database)
            if self.props.readFromSource == "dbtable":
                reader = reader.option("spark.cosmos.container", self.props.container)
            else:
                reader = reader.option("spark.cosmos.read.customQuery", self.props.customQuery)
            if self.props.useGatewayMode is not None:
                reader = reader.option("spark.cosmos.useGatewayMode", self.props.useGatewayMode)
            if self.props.forceEventualConsistency is not None:
                reader = reader.option("spark.cosmos.read.forceEventualConsistency", self.props.forceEventualConsistency)
            if self.props.applicationName is not None:
                reader = reader.option("spark.cosmos.applicationName", self.props.applicationName)
            if self.props.preferredRegionsList is not None:
                reader = reader.option("spark.cosmos.preferredRegionsList", self.props.preferredRegionsList)
            if self.props.disableTcpConnectionEndpointRediscovery is not None:
                reader = reader.option("spark.cosmos.disableTcpConnectionEndpointRediscovery", self.props.disableTcpConnectionEndpointRediscovery)
            if self.props.allowInvalidJsonWithDuplicateJsonProperties is not None:
                reader = reader.option("spark.cosmos.read.allowInvalidJsonWithDuplicateJsonProperties", self.props.allowInvalidJsonWithDuplicateJsonProperties)
            if self.props.maxItemCount is not None:
                reader = reader.option("spark.cosmos.read.maxItemCount", int(self.props.maxItemCount))
            if self.props.useGatewayMode is not None:
                reader = reader.option("spark.cosmos.read.maxIntegratedCacheStalenessInMS", int(self.props.maxIntegratedCacheStalenessInMS))
            # Schema Inference Config
            if self.props.inferSchemaEnabled is not None:
                reader = reader.option("spark.cosmos.read.inferSchema.enabled", self.props.inferSchemaEnabled)
            if self.props.inferSchemaQueryCheckbox and self.props.inferSchemaQuery is not None:
                reader = reader.option("spark.cosmos.read.inferSchema.query", self.props.inferSchemaQuery.value)
            if self.props.inferSchemaSamplingSize is not None:
                reader = reader.option("spark.cosmos.read.inferSchema.samplingSize", self.props.inferSchemaSamplingSize)
            if self.props.inferSchemaIncludeSystemProperties is not None:
                reader = reader.option("spark.cosmos.read.inferSchema.includeSystemProperties", self.props.inferSchemaIncludeSystemProperties)
            if self.props.inferSchemaIncludeTimestamp is not None:
                reader = reader.option("spark.cosmos.read.inferSchema.includeTimestamp", self.props.inferSchemaIncludeTimestamp)
            if self.props.inferSchemaForceNullableProperties is not None:
                reader = reader.option("spark.cosmos.read.inferSchema.forceNullableProperties", self.props.inferSchemaForceNullableProperties)
            # Serialization Config
            if self.props.inclusionMode is not None:
                reader = reader.option("spark.cosmos.serialization.inclusionMode", self.props.inclusionMode)
            if self.props.dateTimeConversionMode is not None:
                reader = reader.option("spark.cosmos.serialization.dateTimeConversionMode", self.props.dateTimeConversionMode)
            # Json conversion configuration
            if self.props.schemaConversionMode is not None:
                reader = reader.option("spark.cosmos.read.schemaConversionMode", self.props.schemaConversionMode)
            # Partitioning Strategy Config
            if self.props.partitioningStrategy is not None:
                reader = reader.option("spark.cosmos.read.partitioning.strategy", self.props.partitioningStrategy)
            if self.props.partitioningTargetedCount is not None:
                reader = reader.option("spark.cosmos.partitioning.targetedCount", self.props.partitioningTargetedCount)
            if self.props.partitioningFeedRangeFilter is not None:
                reader = reader.option("spark.cosmos.partitioning.feedRangeFilter", self.props.partitioningFeedRangeFilter)

            if self.props.throughputControlEnabled is not None:
                reader = reader.option("spark.cosmos.throughputControl.enabled", self.props.throughputControlEnabled)
            if self.props.throughputControlAccountEndpoint is not None:
                reader = reader.option("spark.cosmos.throughputControl.accountEndpoint", self.props.throughputControlAccountEndpoint)
            if self.props.throughputControlAccountKey is not None:
                reader = reader.option("spark.cosmos.throughputControl.accountKey", self.props.throughputControlAccountKey)
            if self.props.throughputControlPreferredRegionsList is not None:
                reader = reader.option("spark.cosmos.throughputControl.preferredRegionsList", self.props.throughputControlPreferredRegionsList)
            if self.props.throughputControlDisableTcpConnectionEndpointRediscovery is not None:
                reader = reader.option("spark.cosmos.throughputControl.disableTcpConnectionEndpointRediscovery", self.props.throughputControlDisableTcpConnectionEndpointRediscovery)
            if self.props.throughputControlUseGatewayMode is not None:
                reader = reader.option("spark.cosmos.throughputControl.useGatewayMode", self.props.throughputControlUseGatewayMode)
            if self.props.throughputControlName is not None:
                reader = reader.option("spark.cosmos.throughputControl.name", self.props.throughputControlName)
            if self.props.throughputControlTargetThroughput is not None:
                reader = reader.option("spark.cosmos.throughputControl.targetThroughput", self.props.throughputControlTargetThroughput)
            if self.props.throughputControlTargetThroughputThreshold is not None:
                reader = reader.option("spark.cosmos.throughputControl.targetThroughputThreshold", self.props.throughputControlTargetThroughputThreshold)
            if self.props.throughputControlGlobalControlDatabase is not None:
                reader = reader.option("spark.cosmos.throughputControl.globalControl.database", self.props.throughputControlGlobalControlDatabase)
            if self.props.throughputControlGlobalControlContainer is not None:
                reader = reader.option("spark.cosmos.throughputControl.globalControl.container", self.props.throughputControlGlobalControlContainer)
            if self.props.throughputControlGlobalControlRenewIntervalInMS is not None:
                reader = reader.option("spark.cosmos.throughputControl.globalControl.renewIntervalInMS", self.props.throughputControlGlobalControlRenewIntervalInMS)
            if self.props.throughputControlGlobalControlExpireIntervalInMS is not None:
                reader = reader.option("spark.cosmos.throughputControl.globalControl.expireIntervalInMS", self.props.throughputControlGlobalControlExpireIntervalInMS)
            if self.props.throughputControlGlobalControlUseDedicatedContainer is not None:
                reader = reader.option("spark.cosmos.throughputControl.globalControl.useDedicatedContainer", self.props.throughputControlGlobalControlUseDedicatedContainer)
            return reader.load()

        def targetApply(self, spark: SparkSession, in0: DataFrame):
            writer = in0.write.format("cosmos.oltp")
            if self.props.authType == "MasterKey":
                writer = writer.option("spark.cosmos.accountEndpoint", self.props.accountEndpoint)
                writer = writer.option("spark.cosmos.accountKey", self.props.accountKey)
                writer = writer.option("spark.cosmos.database", self.props.database)
            elif self.props.authType == "ServicePrincipal":
                writer = writer.option("spark.cosmos.account.subscriptionId	", self.props.subscriptionId)
                writer = writer.option("spark.cosmos.account.tenantId", self.props.tenantId)
                writer = writer.option("spark.cosmos.account.resourceGroupName", self.props.resourceGroupName)
                writer = writer.option("spark.cosmos.auth.aad.clientId", self.props.authClientId)
                writer = writer.option("spark.cosmos.auth.aad.clientSecret", self.props.authClientSecret)
            writer = writer.option("spark.cosmos.account.azureEnvironment", self.props.azureEnvironment)
            writer = writer.option("spark.cosmos.database", self.props.database)
            writer = writer.option("spark.cosmos.container", self.props.container)
            if self.props.throughputControlGlobalControlUseDedicatedContainer is not None:
                writer = writer.option("spark.cosmos.throughputControl.globalControl.useDedicatedContainer", self.props.throughputControlGlobalControlUseDedicatedContainer)
            # Basic Spark Write Options
            if self.props.writeMode is not None:
                writer = writer.mode(self.props.writeMode)
            if self.props.dateFormat is not None:
                writer = writer.option("dateFormat", self.props.dateFormat)
            if self.props.timestampFormat is not None:
                writer = writer.option("timestampFormat", self.props.timestampFormat)
            # Write Configuration
            if self.props.strategy is not None:
                writer = writer.option("spark.cosmos.write.strategy", self.props.strategy)
            if self.props.maxRetryCount is not None:
                writer = writer.option("spark.cosmos.write.maxRetryCount", int(self.props.maxRetryCount))
            if self.props.maxConcurrency is not None:
                writer = writer.option("spark.cosmos.write.point.maxConcurrency", int(self.props.maxConcurrency))
            if self.props.maxPendingOperations is not None:
                writer = writer.option("spark.cosmos.write.bulk.maxPendingOperations", int(self.props.maxPendingOperations))
            if self.props.bulkEnabled is not None:
                writer = writer.option("spark.cosmos.write.bulk.enabled", self.props.bulkEnabled)
            writer.save()