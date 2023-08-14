from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base.ComponentBuilderBase import *
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.StringUtils import isBlank
from prophecy.cb.server.base import WorkflowContext

class RestAPIEnrich(ComponentSpec):
    name: str = "RestAPIEnrich"
    category: str = "Custom"
    gemDescription: str = ""
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/custom/rest-api-enrich"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class RestAPIEnrichProperties(ComponentProperties):
        activeTab: str = "basicTab"
        methodFrom: str = "staticValue"
        method: str = "GET"
        urlFrom: str = "staticValue"
        url: str = (
            "https://www.boredapi.com/api/activity"
        )
        paramsFrom: Optional[str] = None
        params: Optional[str] = None
        dataFrom: Optional[str] = None
        data: Optional[str] = None
        jsonFrom: Optional[str] = None
        json: Optional[str] = None
        headersFrom: Optional[str] = None
        headers: Optional[str] = None
        cookiesFrom: Optional[str] = None
        cookies: Optional[str] = None
        filesFrom: Optional[str] = None
        files: Optional[str] = None
        authFrom: Optional[str] = None
        auth: Optional[str] = None
        timeoutFrom: Optional[str] = None
        timeout: Optional[str] = None
        allow_redirectsFrom: Optional[str] = None
        allow_redirects: Optional[str] = None
        proxiesFrom: Optional[str] = None
        proxies: Optional[str] = None
        verifyFrom: Optional[str] = None
        verify: Optional[str] = None
        streamFrom: Optional[str] = None
        stream: Optional[str] = None
        certFrom: Optional[str] = None
        cert: Optional[str] = None
        awaitTimeFrom: Optional[str] = None
        awaitTime: Optional[str] = None
        parseContent: Optional[bool] = None
        jsonSchemaConfigCheckbox: Optional[bool] = None
        jsonSchemaConfig: Optional[str] = None

    def dialog(self) -> Dialog:
        def create_dialog(heading, from_bind_prop, df_value, static_value, prop):
            return (
                StackLayout()
                    .addElement(TitleElement(title=heading))
                    .addElement(
                    SelectBox("")
                        .addOption("From existing DataFrame column", "existingColumn")
                        .addOption("Static Value", "staticValue")
                        .bindProperty(from_bind_prop)
                )
                    .addElement(
                    (
                        Condition()
                            .ifEqual(
                            PropExpr("component.properties." + from_bind_prop),
                            StringExpr("existingColumn"),
                        )
                            .then(
                            TextBox("", ignoreTitle=True)
                                .bindPlaceholder(df_value)
                                .bindProperty(prop)
                        )
                            .otherwise(
                            TextBox("", ignoreTitle=True)
                                .bindPlaceholder(static_value)
                                .bindProperty(prop)
                        )
                    )
                )
            )

        methodDialog = (
            StackLayout()
                .addElement(TitleElement(title="API Method"))
                .addElement(
                SelectBox("")
                    .addOption("From existing DataFrame column", "existingColumn")
                    .addOption("Static Value", "staticValue")
                    .bindProperty("methodFrom")
            )
                .addElement(
                (
                    Condition()
                        .ifEqual(
                        PropExpr("component.properties.methodFrom"),
                        StringExpr("existingColumn"),
                    )
                        .then(
                        TextBox("", ignoreTitle=True)
                            .bindPlaceholder("method_col")
                            .bindProperty("method")
                    )
                        .otherwise(
                        SelectBox("")
                            .addOption("GET", "GET")
                            .addOption("PUT", "PUT")
                            .addOption("POST", "POST")
                            .addOption("OPTIONS", "OPTIONS")
                            .addOption("HEAD", "HEAD")
                            .addOption("PATCH", "PATCH")
                            .addOption("DELETE", "DELETE")
                            .bindProperty("method")
                    )
                )
            )
        )

        urlDialog = create_dialog(
            "URL",
            "urlFrom",
            "url_col",
            "https://countriesnow.space/api/v0.1/countries/capital",
            "url",
        )

        headersDialog = create_dialog(
            "Headers (Optional)",
            "headersFrom",
            "headers_col",
            '{"key1":"value1", "key2": "value2"}',
            "headers",
        )

        paramsDialog = create_dialog(
            "Params (Optional)",
            "paramsFrom",
            "params_col",
            '{"key1":"value1", "key2": value2}',
            "params",
        )

        dataDialog = create_dialog(
            "Data Body (Optional)",
            "dataFrom",
            "data_col",
            '{"key1":"value1", "key2": value2}',
            "data",
        )

        jsonDialog = create_dialog(
            "JSON Body (Optional)",
            "jsonFrom",
            "json_col",
            '{"key1":"value1", "key2": value2}',
            "json",
        )

        authDialog = create_dialog(
            "Auth (Optional)", "authFrom", "auth_col", "user:pass", "auth"
        )

        timeoutDialog = create_dialog(
            "Timeout (Optional)",
            "timeoutFrom",
            "timeout_col",
            "0.05 or 0.05:0.1",
            "timeout",
        )

        allow_redirectsDialog = create_dialog(
            "Allow Redirects (Optional)",
            "allow_redirectsFrom",
            "allow_redirects_col",
            "true or false",
            "allow_redirects",
        )

        certDialog = create_dialog(
            "Certificate (Optional)",
            "certFrom",
            "cert_col",
            'if String, path to ssl client cert file (.pem). If Tuple, "cert":"key"',
            "cert",
        )

        cookiesDialog = create_dialog(
            "Cookies (Optional)",
            "cookiesFrom",
            "cookies_col",
            '{"key1":"value1", "key2": value2}',
            "cookies",
        )

        verifyDialog = create_dialog(
            "Verify (Optional)",
            "verifyFrom",
            "verify_col",
            "true/false or path to a TLS certificate e.g. : folder/tlscertificate",
            "verify",
        )

        streamDialog = create_dialog(
            "Stream (Optional)", "streamFrom", "stream_col", "true or false", "stream"
        )

        proxiesDialog = create_dialog(
            "Proxies (Optional)",
            "proxiesFrom",
            "proxies_col",
            '{"https" : "https://1.1.0.1:80"}',
            "proxies",
        )

        awaitTimeDialog = create_dialog(
            "Await time after each api call in sec (Optional)",
            "awaitTimeFrom",
            "await_time_col",
            "0.01",
            "awaitTime",
        )

        return Dialog("RestAPIEnrich").addElement(
            ColumnsLayout(gap="1rem", height="100%")
                .addColumn(PortSchemaTabs().importSchema(), "0.5fr")
                .addColumn(
                Tabs()
                    .bindProperty("activeTab")
                    .addTabPane(
                    TabPane("Basic API parameters", "basicTab").addElement(
                        StackLayout("2rem")
                            .addElement(
                            NativeText(
                                "Each property can either be set as static value or value from existing column of the input DataFrame."
                            )
                        )
                            .addElement(methodDialog)
                            .addElement(urlDialog)
                            .addElement(headersDialog)
                            .addElement(paramsDialog)
                            .addElement(jsonDialog)
                            .addElement(dataDialog)
                            .addElement(Checkbox(
                            "Parse content as JSON from sample response record (to make the schema available in output tab for development, please click \"infer schema\" in the output tab and run infer from cluster)").bindProperty(
                            "parseContent"))
                            .addElement(Checkbox(
                            "Parse content as JSON from example record (to make the schema available in output tab for development, please click \"infer schema\" in the output tab and run infer from cluster)").bindProperty(
                            "jsonSchemaConfigCheckbox"))
                            .addElement(
                            Condition()
                                .ifEqual(PropExpr("component.properties.jsonSchemaConfigCheckbox"), BooleanExpr(True))
                                .then(
                                TextArea("Example Record to parse content JSON", 20).bindProperty("jsonSchemaConfig")
                            )
                        )
                    )
                )
                    .addTabPane(
                    TabPane("Other API Parameters", "otherTab").addElement(
                        StackLayout("2rem")
                            .addElement(
                            NativeText(
                                "Each property can either be set as static value or value from existing column of the input DataFrame."
                            )
                        )
                            .addElement(authDialog)
                            .addElement(timeoutDialog)
                            .addElement(allow_redirectsDialog)
                            .addElement(certDialog)
                            .addElement(cookiesDialog)
                            .addElement(verifyDialog)
                            .addElement(streamDialog)
                            .addElement(proxiesDialog)
                    )
                )
                    .addTabPane(
                    TabPane("Advanced Parameters", "advancedTab").addElement(
                        StackLayout("2rem")
                            .addElement(
                            NativeText(
                                "Each property can either be set as static value or value from existing column of the input DataFrame."
                            )
                        )
                            .addElement(awaitTimeDialog)
                    )
                )
            )
        )

    def validate(
            self, context: WorkflowContext, component: Component[RestAPIEnrichProperties]
    ) -> List[Diagnostic]:
        diagnostics = []
        props = component.properties
        if isBlank(props.method):
            diagnostics.append(
                Diagnostic(
                    "properties.method",
                    "Method cannot be empty",
                    SeverityLevelEnum.Error,
                )
            )
        if isBlank(props.url):
            diagnostics.append(
                Diagnostic(
                    "properties.url", "URL cannot be empty", SeverityLevelEnum.Error
                )
            )
        if props.jsonSchemaConfigCheckbox and props.parseContent:
            diagnostics.append(
                Diagnostic(
                    "properties.parseContent",
                    "Only one option among parse JSON from response record or parse JSON from example reocrd can be turned on.",
                    SeverityLevelEnum.Error
                )
            )
        return diagnostics

    def onChange(
            self,
            context: WorkflowContext,
            oldState: Component[RestAPIEnrichProperties],
            newState: Component[RestAPIEnrichProperties],
    ) -> Component[RestAPIEnrichProperties]:
        oldProps = oldState.properties
        newProps = newState.properties

        method = None if oldProps.methodFrom != newProps.methodFrom else newProps.method
        url = None if oldProps.urlFrom != newProps.urlFrom else newProps.url
        params = None if oldProps.paramsFrom != newProps.paramsFrom else newProps.params
        data = None if oldProps.dataFrom != newProps.dataFrom else newProps.data
        json = None if oldProps.jsonFrom != newProps.jsonFrom else newProps.json
        headers = (
            None if oldProps.headersFrom != newProps.headersFrom else newProps.headers
        )

        return newState.bindProperties(
            replace(
                newProps,
                activeTab=newProps.activeTab,
                method=method,
                url=url,
                params=params,
                data=data,
                json=json,
                headers=headers,
            )
        )

    class RestAPIEnrichCode(ComponentCode):
        def __init__(self, newProps):
            self.props: RestAPIEnrich.RestAPIEnrichProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            from prophecy.udfs import get_rest_api
            method = (
                col(self.props.method).alias("method")
                if self.props.methodFrom == "existingColumn"
                else lit(self.props.method).alias("method")
            )
            url = (
                col(self.props.url).alias("url")
                if self.props.urlFrom == "existingColumn"
                else lit(self.props.url).alias("url")
            )

            udf_col_list = [method, url]

            if self.props.params is not None:
                params = (
                    col(self.props.params).alias("params")
                    if self.props.paramsFrom == "existingColumn"
                    else lit(self.props.params).alias("params")
                )
                udf_col_list.append(params)

            if self.props.data is not None:
                data = (
                    col(self.props.data).alias("data")
                    if self.props.dataFrom == "existingColumn"
                    else lit(self.props.data).alias("data")
                )
                udf_col_list.append(data)
            if self.props.json is not None:
                json = (
                    col(self.props.json).alias("json")
                    if self.props.jsonFrom == "existingColumn"
                    else lit(self.props.json).alias("json")
                )
                udf_col_list.append(json)
            if self.props.headers is not None:
                headers = (
                    col(self.props.headers).alias("headers")
                    if self.props.headersFrom == "existingColumn"
                    else lit(self.props.headers).alias("headers")
                )
                udf_col_list.append(headers)
            if self.props.cookies is not None:
                cookies = (
                    col(self.props.cookies).alias("cookies")
                    if self.props.cookiesFrom == "existingColumn"
                    else lit(self.props.cookies).alias("cookies")
                )
                udf_col_list.append(cookies)
            # if self.props.files is not None:
            #     files = (
            #         col(self.props.files).alias('files')
            #         if self.props.filesFrom == "existingColumn"
            #         else lit(self.props.files).alias('files')
            #     )
            #     udf_col_list.append(files)
            if self.props.auth is not None:
                auth = (
                    col(self.props.auth).alias("auth")
                    if self.props.authFrom == "existingColumn"
                    else lit(self.props.auth).alias("auth")
                )
                udf_col_list.append(auth)
            if self.props.timeout is not None:
                timeout = (
                    col(self.props.timeout).alias("timeout")
                    if self.props.timeoutFrom == "existingColumn"
                    else lit(self.props.timeout).alias("timeout")
                )
                udf_col_list.append(timeout)
            if self.props.allow_redirects is not None:
                allow_redirects = (
                    col(self.props.allow_redirects).alias("allow_redirects")
                    if self.props.allow_redirectsFrom == "existingColumn"
                    else lit(self.props.allow_redirects).alias("allow_redirects")
                )
                udf_col_list.append(allow_redirects)
            if self.props.proxies is not None:
                proxies = (
                    col(self.props.proxies).alias("proxies")
                    if self.props.proxiesFrom == "existingColumn"
                    else lit(self.props.proxies).alias("proxies")
                )
                udf_col_list.append(proxies)
            if self.props.verify is not None:
                verify = (
                    col(self.props.verify).alias("verify")
                    if self.props.verifyFrom == "existingColumn"
                    else lit(self.props.verify).alias("verify")
                )
                udf_col_list.append(verify)
            if self.props.stream is not None:
                stream = (
                    col(self.props.stream).alias("stream")
                    if self.props.streamFrom == "existingColumn"
                    else lit(self.props.stream).alias("stream")
                )
                udf_col_list.append(stream)
            if self.props.cert is not None:
                cert = (
                    col(self.props.cert).alias("cert")
                    if self.props.certFrom == "existingColumn"
                    else lit(self.props.cert).alias("cert")
                )
                udf_col_list.append(cert)

            awaitTime = (
                col(self.props.awaitTime)
                if self.props.certFrom == "existingColumn"
                else lit(self.props.awaitTime) if self.props.awaitTime is not None else lit("")
            )

            requestDF: SubstituteDisabled = in0.withColumn(
                "api_output", get_rest_api(to_json(struct(*udf_col_list)), awaitTime)
            )

            if self.props.parseContent:
                responseFieldName = "api_output.content"
                listOfRows = requestDF.select(responseFieldName).take(1)
                schema = schema_of_json(listOfRows[0][0])
                out0 = requestDF.withColumn("content_parsed", from_json(col(responseFieldName), schema))
                return out0
            if self.props.jsonSchemaConfigCheckbox:
                responseFieldName = "api_output.content"
                schema = schema_of_json(self.props.jsonSchemaConfig)
                out0 = requestDF.withColumn("content_parsed", from_json(col(responseFieldName), schema))
                return out0
            else:
                return requestDF
