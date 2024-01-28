const express = require("express");
const bodyParser = require("body-parser");
const AWS = require("aws-sdk");
const { Pool } = require("pg");

AWS.config.update({
  region: "us-west-2",
  signatureVersion: "v4",
});

const app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

const appflow = new AWS.Appflow({ apiVersion: "2020-08-23" });

let init = false;
let pool = null;

const parseSecrets = async (secretString) => {
  const parsedSecrets = JSON.parse(secretString);
  for (const key in parsedSecrets) {
    if (parsedSecrets.hasOwnProperty(key)) {
      const innerString = parsedSecrets[key];
      parsedSecrets[key] = JSON.parse(innerString);
    }
  }
  return parsedSecrets;
};

const fetchSecrets = async () => {
  try {
    const secretsManager = new AWS.SecretsManager();
    const DEV_SECRET_NAME = "dev/envs";

    const data = await secretsManager
      .getSecretValue({ SecretId: DEV_SECRET_NAME })
      .promise();
    const secretString = data.SecretString;
    if (secretString) {
      const parsedSecrets = await parseSecrets(secretString);
      return parsedSecrets;
    } else {
      throw new Error("SecretString is empty");
    }
  } catch (error) {
    console.error("Error retrieving secret:", error);
    throw error;
  }
};

const getDatabasePool = async (event) => {
  try {
    if (!init) {
      const { DB_CREDENTIALS } = await fetchSecrets();

      pool = new Pool({
        user: DB_CREDENTIALS.user,
        host: DB_CREDENTIALS.host,
        database: DB_CREDENTIALS.database,
        password: DB_CREDENTIALS.password,
        port: DB_CREDENTIALS.port,
        idleTimeoutMillis: 10000000,
        connectionTimeoutMillis: 1000000,
      });
      init = true;
    }

    if (!pool) {
      throw new Error("Env not initialized.");
    }

    return pool;
  } catch (error) {
    console.error("Error", error);
  }
};

getDatabasePool();

const updateFlowStatus = async (
  flow_id,
  flow_name,
  flow_event_type,
  connection_profile_name,
  user_id,
  pool,
  connectorApiVersion
) => {
  try {
    const flowDetails = await appflow
      .describeFlow({ flowName: flow_name })
      .promise();

    if (flow_event_type === "LinkSend" || flow_event_type === "Link") return;

    const timestampField = await findTimestampFieldIdentifierForStatusUpdate(
      connection_profile_name,
      connectorApiVersion,
      flow_event_type
    );

    const flow = await appflow
      .updateFlow({
        flowName: flow_name,
        description: "",
        triggerConfig: {
          triggerType: "Scheduled",
          triggerProperties: {
            Scheduled: {
              scheduleExpression: "rate(1hours)",
              dataPullMode: "Incremental",
              scheduleStartTime: Math.floor(
                (Date.now() + 2 * 60 * 1000) / 1000
              ),
              timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
              scheduleOffset: 0,
            },
          },
        },
        sourceFlowConfig: {
          connectorType: "CustomConnector",
          connectorProfileName: connection_profile_name,
          apiVersion: connectorApiVersion,
          sourceConnectorProperties: {
            CustomConnector: {
              entityName: flow_event_type,
              customProperties: {},
            },
          },
          incrementalPullConfig: {
            datetimeTypeFieldName: timestampField,
          },
        },
        destinationFlowConfigList: [
          {
            connectorType: "S3",
            destinationConnectorProperties: {
              S3: {
                bucketName: "palisade-api-connectors",
                bucketPrefix: user_id + `/${connection_profile_name}`,
                s3OutputFormatConfig: {
                  fileType: "JSON",
                  prefixConfig: {
                    prefixFormat: "DAY", // Partitioned Datalake format <YEAR>/<MONTH>/<DATE>/<DAILY-CONSOLIDATED-DATA>
                    prefixType: "PATH",
                    pathPrefixHierarchy: ["EXECUTION_ID"],
                  },
                  aggregationConfig: {
                    aggregationType: "SingleFile",
                  },
                },
              },
            },
          },
        ],
        tasks: flowDetails.tasks,
        metadataCatalogConfig: {},
      })
      .promise();

    const updateFLowInDbQuery = `update
        api_connectors.flows
      set
        flow_type = 'INCREMENTAL'
      where
        id = $1`;

    const updateFlowInDb = await pool.query(updateFLowInDbQuery, [flow_id]);

    return;
  } catch (error) {
    console.error(error);
  }
};

const findTimestampFieldIdentifierForStatusUpdate = async (
  connection_profile_name,
  connectorApiVersion,
  flow_event_type
) => {
  if (["NotSentEvent", "SentEvent"].includes(flow_event_type)) {
    return "EventDate";
  }

  if (["sales_accounts", "contacts"].includes(flow_event_type)) {
    return "updated_at";
  }

  const { connectorEntityFields } = await appflow
    .describeConnectorEntity({
      connectorProfileName: connection_profile_name,
      connectorType: "CustomConnector",
      connectorEntityName: flow_event_type,
      apiVersion: connectorApiVersion,
    })
    .promise();

  const queryableEntityFields = connectorEntityFields.filter(
    (entity) => entity.sourceProperties.isQueryable
  );

  for (const field of queryableEntityFields) {
    if (
      field.sourceProperties &&
      field.sourceProperties.isTimestampFieldForIncrementalQueries
    ) {
      return field.identifier;
    }
  }
  return null; // Return null if no match is found
};

app.get("/", async (req, res) => {
  return res.send("Server is up and running!!!");
});

app.post("/flow-status-update", async (req, res) => {
  try {
    const flowExecutionRecord = req.body;

    if (
      flowExecutionRecord.detail &&
      flowExecutionRecord.detail.hasOwnProperty("status")
    ) {
      // const pool = await getDatabasePool();
      const getFlowFromDBQuery = `SELECT
          api_connectors.flows.*,
          api_connectors.connections.name as connection_name,
          api_connectors.connections.user_id,
          api_connectors.connections.connector_id,
          api_connectors.connections.status as connection_status
      FROM
          api_connectors.flows
      JOIN
          api_connectors.connections ON api_connectors.flows.connection_id = api_connectors.connections.id
      WHERE
          api_connectors.flows.name = $1;
      `;
      const fetchFlow = await pool.query(getFlowFromDBQuery, [
        flowExecutionRecord.detail["flow-name"],
      ]);

      if (fetchFlow.rows.length > 0) {
        const flow = fetchFlow.rows[0];

        const flowExecutionDetails = await appflow
          .describeFlowExecutionRecords({ flowName: flow.name })
          .promise();

        if (flow.flow_type === "HISTORICAL") {
          const connectorApiVersion = {
            1: "v1",
            5: "v1.0",
          };

          const updateFlow = await updateFlowStatus(
            flow.id,
            flow.name,
            flow.event_type,
            flow.connection_name,
            flow.user_id,
            pool,
            connectorApiVersion[flow.connector_id]
          );
        }

        const lastExecution = flowExecutionDetails.flowExecutions.find(
          (item) =>
            item.executionId === flowExecutionRecord.detail["execution-id"]
        );

        let startTime = new Date(lastExecution.startedAt);
        let endTime = new Date(lastExecution.lastUpdatedAt);

        let executionTimeSeconds = (endTime - startTime) / 1000;

        if (lastExecution.executionId !== flow.last_execution_id) {
          if (lastExecution.executionStatus === "Error") {
            const flowDetails = await appflow
              .describeFlow({ flowName: flow.name })
              .promise();

            const updateFlowQuery = `update api_connectors.flows set status = $1, last_execution_time = $2, last_execution_status = $3, last_execution_records = last_execution_records + $4, total_execution_time = $5, last_execution_id = $6 where id = $7`;

            const updateFlow = await pool.query(updateFlowQuery, [
              flowDetails.flowStatus,
              lastExecution.dataPullEndTime,
              lastExecution.executionStatus,
              lastExecution.executionResult.recordsProcessed,
              executionTimeSeconds,
              lastExecution.executionId,
              flow.id,
            ]);
          } else {
            const updateFlowQuery = `update api_connectors.flows set last_execution_time = $1, last_execution_status = $2, last_execution_records = last_execution_records + $3, total_execution_time = $4, last_execution_id = $5 where id =$6`;

            const updateFlow = await pool.query(updateFlowQuery, [
              lastExecution.dataPullEndTime,
              lastExecution.executionStatus,
              lastExecution.executionResult.recordsProcessed,
              executionTimeSeconds,
              lastExecution.executionId,
              flow.id,
            ]);
          }
        }
      }
      return res.json({
        message: "Flow Updated",
      });
    }
  } catch (error) {
    console.log(error);
    return res.json(error);
  }
});

app.listen(1313, () => {
  console.log("Server is running on port 1313");
});
