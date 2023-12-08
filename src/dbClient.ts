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

import {
  createConnection,
  Connection,
  RowDataPacket,
  ResultSetHeader,
  ProcedureCallPacket,
  FieldPacket
} from 'mysql2/promise';
import { getMysqlConnectionOptions, USE_READ_COMMITTED_ISOLATION } from './config';
import { ConnectionOptions } from 'mysql2';

interface ConnectionWrap extends Connection {
  run<T extends ResultSetHeader | ResultSetHeader[] | RowDataPacket[] | RowDataPacket[][] | ProcedureCallPacket>(
    sql: string,
    phs?: (string | number | boolean | null)[]
  ): Promise<{
    rows: T;
    cols: FieldPacket[];
  }>;
}

export class DbClient {
  private connection?: ConnectionWrap;
  private readonly connectionOptions: ConnectionOptions;

  constructor(connectionOptionsOrPrefix?: ConnectionOptions | string) {
    if (connectionOptionsOrPrefix) {
      if (typeof connectionOptionsOrPrefix === 'string') {
        this.connectionOptions = getMysqlConnectionOptions(connectionOptionsOrPrefix);
      } else {
        this.connectionOptions = connectionOptionsOrPrefix;
      }
    } else {
      this.connectionOptions = getMysqlConnectionOptions('');
    }
  }

  async open() {
    this.connection = (await createConnection(this.connectionOptions)) as ConnectionWrap;
    this.connection.run = async <
      T extends ResultSetHeader | ResultSetHeader[] | RowDataPacket[] | RowDataPacket[][] | ProcedureCallPacket
    >(
      sql: string,
      phs?: (string | number | boolean | null)[]
    ): Promise<{ rows: T; cols: FieldPacket[] }> => {
      if (!this.connection) {
        throw new Error('run() called but no connection available');
      }
      const [rows, cols] = await this.connection.execute<T>(sql, phs);
      return {
        rows,
        cols
      };
    };
  }

  getConnection(): ConnectionWrap | undefined {
    return this.connection;
  }

  async close() {
    if (this.connection) {
      await this.connection.end();
    }
  }

  async startTransaction() {
    if (!this.connection) {
      throw new Error('no connection available');
    }
    if (USE_READ_COMMITTED_ISOLATION) {
      await this.connection.execute("SET SESSION tx_isolation='read-committed'");
    }
    await this.connection.execute('SET AUTOCOMMIT=0');
  }

  async commit(closeTransaction = true) {
    if (!this.connection) {
      throw new Error('no connection available');
    }
    await this.connection.execute('COMMIT');
    if (closeTransaction) {
      await this.connection.execute('SET AUTOCOMMIT=1');
    }
  }

  async rollback(closeTransaction = true) {
    if (!this.connection) {
      throw new Error('no connection available');
    }

    await this.connection.execute('ROLLBACK');
    if (closeTransaction) {
      await this.connection.execute('SET AUTOCOMMIT=1');
    }
  }

  async all(sql: string, phs?: (string | number | boolean | null)[]) {
    if (!this.connection) {
      throw new Error('no connection available');
    }
    const res = await this.connection.run<RowDataPacket[]>(sql, phs);
    return res.rows;
  }

  async get(sql: string, phs?: (string | number | boolean | null)[]): Promise<RowDataPacket | null> {
    if (!this.connection) {
      throw new Error('no connection available');
    }
    const res = await this.connection.run<RowDataPacket[]>(sql, phs);
    if (res.rows.length > 1) throw new Error('get() returned more than one row');
    if (res.rows.length === 1) return res.rows[0];
    return null;
  }

  async insert(sql: string, phs?: (string | number | boolean | null)[]) {
    if (!this.connection) {
      throw new Error('no connection available');
    }
    const res = await this.connection.run<ResultSetHeader>(sql, phs);
    return res.rows.insertId || res.rows.affectedRows;
  }

  async update(sql: string, phs?: (string | number | boolean | null)[]) {
    if (!this.connection) {
      throw new Error('no connection available');
    }
    const res = await this.connection.run<ResultSetHeader>(sql, phs);
    return res.rows.affectedRows;
  }

  async delete(sql: string, phs?: (string | number | boolean | null)[]) {
    if (!this.connection) {
      throw new Error('no connection available');
    }
    const res = await this.connection.run<ResultSetHeader>(sql, phs);
    return res.rows.affectedRows;
  }

  async run(sql: string, phs?: (string | number | boolean | null)[]) {
    if (!this.connection) {
      throw new Error('no connection available');
    }
    return this.connection.execute(sql, phs);
  }
}
