/*
 * Copyright Fluidware srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import type { ConnectionOptions } from 'mysql2';
import { EnvParse } from '@fluidware-it/saddlebag';
import * as fs from 'fs';

export const USE_READ_COMMITTED_ISOLATION = EnvParse.envBool('FW_DB_USE_READ_COMMITTED_ISOLATION', false);

function getDbPassword(DB_PASSWORD_FILE: string, DB_PASSWORD: string): string {
  if (DB_PASSWORD_FILE) {
    return fs.readFileSync(DB_PASSWORD_FILE, 'utf8');
  }
  return DB_PASSWORD;
}

const memoizedOptions: { [prefix: string]: ConnectionOptions } = {};

export function getMysqlConnectionOptions(prefix = ''): ConnectionOptions {
  if (!memoizedOptions[prefix]) {
    const DB_PASSWORD = EnvParse.envString(`FW_${prefix}DB_PASSWORD`, '');
    const DB_PASSWORD_FILE = EnvParse.envString(`FW_${prefix}DB_PASSWORD_FILE`, '');

    if (!DB_PASSWORD && !DB_PASSWORD_FILE) {
      throw new Error('FW_DB_PASSWORD or FW_DB_PASSWORD_FILE env is required');
    }

    const DB_USER = EnvParse.envStringRequired(`FW_${prefix}DB_USER`);
    const DB_HOST = EnvParse.envString(`FW_${prefix}DB_HOST`, 'localhost');
    const DB_PORT = EnvParse.envInt(`FW_${prefix}DB_PORT`, 3306);
    const DB_NAME = EnvParse.envString(`FW_${prefix}DB_NAME`, DB_USER);
    // FW_${prefix}DB_CONN_OPTIONS: JSON string with connection options See https://github.com/mysqljs/mysql#connection-options for all possible options
    const DB_CONN_OPTIONS = EnvParse.envJSON(`FW_${prefix}DB_CONN_OPTIONS`, {});

    const dbPassword = getDbPassword(DB_PASSWORD_FILE, DB_PASSWORD);
    const dbOptions = {
      host: DB_HOST,
      port: DB_PORT,
      user: DB_USER,
      password: dbPassword,
      database: DB_NAME
    };
    if (Object.keys(DB_CONN_OPTIONS).length > 0) {
      Object.assign(dbOptions, DB_CONN_OPTIONS);
    }
    memoizedOptions[prefix] = dbOptions;
  }
  return memoizedOptions[prefix];
}

export function setMysqlConnectionOptions(prefix: string, options: ConnectionOptions) {
  memoizedOptions[prefix] = options;
}
