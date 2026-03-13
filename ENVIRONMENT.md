
## Environment variables

| ENV                                |              type |    default | required |                                                                                                                notes |
| ---------------------------------- | ----------------- | ---------- | -------- | -------------------------------------------------------------------------------------------------------------------- |
| FW_${prefix}DB_CONN_OPTIONS        | unknown (envJSON) |            |          | JSON string with connection options See https://github.com/mysqljs/mysql#connection-options for all possible options |
| FW_${prefix}DB_HOST                |            string |  localhost |          |                                                                                                                      |
| FW_${prefix}DB_NAME                |            string | ${DB_USER} |          |                                                                                                                      |
| FW_${prefix}DB_PASSWORD            |            string |            |          |                                                                                                                      |
| FW_${prefix}DB_PASSWORD_FILE       |            string |            |          |                                                                                                                      |
| FW_${prefix}DB_PORT                |           integer |       3306 |          |                                                                                                                      |
| FW_${prefix}DB_USER                |            string |            |        * |                                                                                                                      |
| FW_DB_USE_READ_COMMITTED_ISOLATION |           boolean |      false |          |                                                                                                                      |

## @fluidware-it/saddlebag@0.3.0

| ENV                          |     type |                 default | required |    notes |
| ---------------------------- | -------- | ----------------------- | -------- | -------- |
| FW_LOGGER_ISO_TIMESTAMP      |  boolean |                   false |          |          |
| FW_LOGGER_LEVEL              |   string |                    info |          |          |
| FW_LOGGER_NAME               |   string |              _function_ |          |          |
| FW_LOGGER_REDACT_KEYS        | string[] |                         |          |          |
| FW_LOGGER_SEVERITY_AS_STRING |  boolean |                   false |          |          |
| npm_package_name             |   string | @fluidware-it/saddlebag |          |          |
