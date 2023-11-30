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
import { setTimeout } from 'node:timers/promises';
import { getLogger } from '@fluidware-it/saddlebag';
import { DbClient } from './dbClient';
import { Logger } from 'pino';

export type UpgradeManagerConfig = {
  version_table_suffix?: string;
};

export class UpgradeManager {
  private version_table: string;
  private client: DbClient;
  private logger: Logger;
  private _default_version_table = '_version';

  constructor(opts?: UpgradeManagerConfig) {
    this.version_table = this._default_version_table + (opts?.version_table_suffix || '');
    this.logger = getLogger().child({ component: 'mysql-migrator' });
    this.client = new DbClient();
  }

  private async loadCurrentVersion(targetVersion: number) {
    try {
      const row = await this.getCurrentVersion(targetVersion);
      if (row === false) {
        return -999;
      } else if (row === true) {
        this.logger.info('Check db: same version %s', targetVersion);
        await this.client.close();
        return true;
      } else if (row) {
        return row.value;
      }
    } catch (e) {
      this.logger.error(`Failed to read current version: ${e.message}`);
      throw e;
    }
  }

  private async createDb() {
    try {
      return this.createVersionTable();
    } catch (e) {
      if (e.code === '23505') {
        await this.client.close();
        await setTimeout(2000);
        return -1;
      } else {
        this.logger.error(`Unable to create ${this.version_table} table: [${e.code}] ${e.message}`);
        throw e;
      }
    }
  }

  private async _checkDb(
    currentVersion: number,
    targetVersion: number,
    onSchemaInit: (dbClient: DbClient) => Promise<void>,
    onSchemaUpgrade: (dbClient: DbClient, from: number) => Promise<void>
  ) {
    try {
      if (currentVersion === 0) {
        await this.initDb(targetVersion, onSchemaInit);
        this.logger.info('Db created');
        return true;
      }
      if (targetVersion === currentVersion) {
        return true;
      }
      if (targetVersion > currentVersion) {
        await this.upgradeDb(currentVersion, targetVersion, onSchemaUpgrade);
        this.logger.info('Db updated to ', targetVersion);
        return true;
      }
    } catch (e) {
      this.logger.error('\n');
      this.logger.error('\n');
      this.logger.error(' !!!!!!!! FATAL ERROR !!!!!!!!');
      this.logger.error('\n');
      this.logger.error('checkDb failed %s', e.message);
      this.logger.error('\n');
      this.logger.error(' !!!!!!!! FATAL ERROR !!!!!!!!');
      this.logger.error('\n');
      this.logger.error('\n');
      this.logger.error(e.stack);
      throw e;
    } finally {
      await this.client.close();
    }
    return true;
  }

  private async getCurrentVersion(targetVersion: number) {
    const sqlCheck = `select value from ${this.version_table} for update`;
    await this.client.startTransaction();
    let row;
    try {
      row = await this.client.get(sqlCheck);
      this.logger.debug('Versions: %s vs %s', row.value, targetVersion);
      if (row.value === targetVersion) {
        this.logger.debug('versions are equal, rollback');
        await this.client.rollback();
        return true;
      }
      return row;
    } catch (e) {
      await this.client.rollback();
      if (e.code === 'ER_NO_SUCH_TABLE') {
        return false;
      }
      this.logger.error('Ooops: %s', e.message, e.code);
      throw e;
    }
  }

  private async createVersionTable() {
    const sqlCreateTable = `create table ${this.version_table} (value INTEGER PRIMARY KEY)`;
    try {
      await this.client.run(sqlCreateTable);
    } catch (e) {
      await this.client.rollback();
      throw e;
    }
    try {
      const sqlInsertVersionZero = `insert into ${this.version_table} values (-1)`;
      await this.client.insert(sqlInsertVersionZero);
      await this.client.get(`select value from ${this.version_table} for update`);
    } catch (e) {
      this.logger.error('Unable to insert -1 version in %s table: [%s] %s', this.version_table, e.code, e.message);
      throw e;
    }
    return 0;
  }

  private async updateVersion(targetVersion: number) {
    const sql = `update ${this.version_table} set value = ?`;
    await this.client.update(sql, [targetVersion]);
    await this.client.commit();
  }

  private async initDb(targetVersion: number, onInit: (dbClient: DbClient) => Promise<void>) {
    const dbConn = new DbClient();
    try {
      await dbConn.open();
      await onInit(dbConn);
      await this.updateVersion(targetVersion);
    } finally {
      if (dbConn) {
        await dbConn.close();
      }
    }
  }

  private async upgradeDb(
    fromVersion: number,
    targetVersion: number,
    onUpgrade: (dbClient: DbClient, from: number) => Promise<void>
  ) {
    await onUpgrade(this.client, fromVersion);
    await this.updateVersion(targetVersion);
  }

  async checkDb(
    targetVersion: number,
    onSchemaInit: (dbClient: DbClient) => Promise<void>,
    onSchemaUpgrade: (dbClient: DbClient, from: number) => Promise<void>
  ) {
    await this.client.open();
    let currentVersion = await this.loadCurrentVersion(targetVersion);
    if (currentVersion === true) {
      return true;
    }
    this.logger.info('Check db: currentVersion %s targetVersion %s', currentVersion, targetVersion);

    if (currentVersion === -999) {
      currentVersion = await this.createDb();
      if (currentVersion < 0) {
        await this.checkDb(targetVersion, onSchemaInit, onSchemaUpgrade);
        return;
      }
    } else if (currentVersion < 0) {
      this.logger.error('Db in initialization, exiting ');
      throw new Error('Db in initialization, do not proceed');
    }
    return this._checkDb(currentVersion, targetVersion, onSchemaInit, onSchemaUpgrade);
  }
}
