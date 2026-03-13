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

import { DbClient, UpgradeManager } from '../src';
import { MariaDbContainer, StartedMariaDbContainer } from '@testcontainers/mariadb';
import { setTimeout } from 'node:timers/promises';

const MARIADB_IMAGE = 'mariadb:11.3.2';

describe('UpgradeManager', function () {
  jest.setTimeout(240_000);

  let mariadbContainer: StartedMariaDbContainer;
  let currentEnv: NodeJS.ProcessEnv;
  let dbClient: DbClient;

  beforeAll(async () => {
    currentEnv = process.env;

    mariadbContainer = await new MariaDbContainer(MARIADB_IMAGE).withExposedPorts(3306).start();

    process.env.FW_DB_CONN_OPTIONS = '{"timezone":"Z"}';
    process.env.FW_DB_NAME = mariadbContainer.getDatabase();
    process.env.FW_DB_HOST = '127.0.0.1';
    process.env.FW_DB_PORT = `${mariadbContainer.getMappedPort(3306)}`;
    process.env.FW_DB_USER = mariadbContainer.getUsername();
    process.env.FW_DB_PASSWORD = mariadbContainer.getUserPassword();

    dbClient = new DbClient();
    await dbClient.open();
  });

  afterAll(async () => {
    await mariadbContainer.stop();

    await dbClient.close();
    process.env = currentEnv;
  });

  it('must migrate from empty database', async () => {
    const upgradeManager = new UpgradeManager({
      version_table_suffix: '_test'
    });
    let onSchemaInitCalled = false;
    let onSchemaUpdateCalled = false;
    await upgradeManager.checkDb(
      1,
      async (_dbClient: DbClient) => {
        onSchemaInitCalled = true;
      },
      async (_dbClient: DbClient, _fromVersion: number) => {
        onSchemaUpdateCalled = true;
      }
    );
    expect(onSchemaInitCalled).toBe(true);
    expect(onSchemaUpdateCalled).toBe(false);
    const versionRow = await dbClient.get('SELECT value FROM _version_test');
    expect(versionRow).toBeDefined();
    expect(versionRow!.value).toBe(1);
  });

  it('must migrate from existing database', async () => {
    const upgradeManager = new UpgradeManager({
      version_table_suffix: '_test2'
    });
    let onSchemaInitCalled = false;
    let onSchemaUpdateCalled = false;
    await upgradeManager.checkDb(
      1,
      async (_dbClient: DbClient) => {
        onSchemaInitCalled = true;
      },
      async (_dbClient: DbClient, _fromVersion: number) => {
        onSchemaUpdateCalled = true;
      }
    );
    expect(onSchemaInitCalled).toBe(true);
    expect(onSchemaUpdateCalled).toBe(false);
    const versionRow = await dbClient.get('SELECT value FROM _version_test2');
    expect(versionRow).toBeDefined();
    expect(versionRow!.value).toBe(1);

    onSchemaInitCalled = false;
    onSchemaUpdateCalled = false;
    let fromVersion = -Infinity;
    await upgradeManager.checkDb(
      2,
      async (_dbClient: DbClient) => {
        onSchemaInitCalled = true;
      },
      async (_dbClient: DbClient, _fromVersion: number) => {
        onSchemaUpdateCalled = true;
        fromVersion = _fromVersion;
      }
    );
    expect(onSchemaInitCalled).toBe(false);
    expect(onSchemaUpdateCalled).toBe(true);
    expect(fromVersion).toBe(1);
    const versionRow2 = await dbClient.get('SELECT value FROM _version_test2');
    expect(versionRow2).toBeDefined();
    expect(versionRow2!.value).toBe(2);
  });

  it('must migrate from empty database: 2 managers', async () => {
    const upgradeManager = new UpgradeManager({
      version_table_suffix: '_test3'
    });
    const upgradeManager2 = new UpgradeManager({
      version_table_suffix: '_test3'
    });
    let onSchemaInitCalled1 = false;
    let onSchemaUpdateCalled1 = false;
    let onSchemaInitCalled2 = false;
    let onSchemaUpdateCalled2 = false;
    const results = await Promise.allSettled([
      upgradeManager.checkDb(
        1,
        async (_dbClient: DbClient) => {
          onSchemaInitCalled1 = true;
        },
        async (_dbClient: DbClient, _fromVersion: number) => {
          onSchemaUpdateCalled1 = true;
        }
      ),
      upgradeManager2.checkDb(
        1,
        async (_dbClient: DbClient) => {
          onSchemaInitCalled2 = true;
        },
        async (_dbClient: DbClient, _fromVersion: number) => {
          onSchemaUpdateCalled2 = true;
        }
      )
    ]);
    // all proms must be fulfilled
    for (const result of results) {
      expect(result.status).toBe('fulfilled');
    }

    // only one between onSchemaInitCalled1 and onSchemaInitCalled2 must be true, the other must be false,
    // because only one of the two managers should have created the version table and called onSchemaInit

    expect((onSchemaInitCalled1 && !onSchemaInitCalled2) || (!onSchemaInitCalled1 && onSchemaInitCalled2)).toBe(true);
    expect(onSchemaUpdateCalled1).toBe(false);
    expect(onSchemaUpdateCalled2).toBe(false);
    const versionRow = await dbClient.get('SELECT value FROM _version_test3');
    expect(versionRow).toBeDefined();
    expect(versionRow!.value).toBe(1);
  });

  it('must migrate from existing database: 2 managers', async () => {
    const upgradeManager = new UpgradeManager({
      version_table_suffix: '_test4'
    });
    const upgradeManager2 = new UpgradeManager({
      version_table_suffix: '_test4'
    });
    let onSchemaInitCalled = false;
    let onSchemaUpdateCalled = false;
    await upgradeManager.checkDb(
      1,
      async (_dbClient: DbClient) => {
        onSchemaInitCalled = true;
      },
      async (_dbClient: DbClient, _fromVersion: number) => {
        onSchemaUpdateCalled = true;
      }
    );
    expect(onSchemaInitCalled).toBe(true);
    expect(onSchemaUpdateCalled).toBe(false);
    const versionRow = await dbClient.get('SELECT value FROM _version_test4');
    expect(versionRow).toBeDefined();
    expect(versionRow!.value).toBe(1);
    let onSchemaInitCalled1 = false;
    let onSchemaUpdateCalled1 = false;
    let onSchemaInitCalled2 = false;
    let onSchemaUpdateCalled2 = false;
    const results = await Promise.allSettled([
      upgradeManager.checkDb(
        2,
        async (_dbClient: DbClient) => {
          onSchemaInitCalled1 = true;
        },
        async (_dbClient: DbClient, _fromVersion: number) => {
          onSchemaUpdateCalled1 = true;
        }
      ),
      upgradeManager2.checkDb(
        2,
        async (_dbClient: DbClient) => {
          onSchemaInitCalled2 = true;
        },
        async (_dbClient: DbClient, _fromVersion: number) => {
          onSchemaUpdateCalled2 = true;
        }
      )
    ]);
    // both proms must be fulfilled
    for (const result of results) {
      expect(result.status).toBe('fulfilled');
    }
    expect(onSchemaInitCalled1 && onSchemaInitCalled2).toBe(false);
    expect((onSchemaUpdateCalled1 && !onSchemaUpdateCalled2) || (!onSchemaUpdateCalled1 && onSchemaUpdateCalled2)).toBe(
      true
    );
    const versionRow2 = await dbClient.get('SELECT value FROM _version_test4');
    expect(versionRow2).toBeDefined();
    expect(versionRow2!.value).toBe(2);
  });

  it('must migrate from empty database: 2 managers taking a while to execute onSchemaInit', async () => {
    const upgradeManager = new UpgradeManager({
      version_table_suffix: '_test5'
    });
    const upgradeManager2 = new UpgradeManager({
      version_table_suffix: '_test5'
    });
    let onSchemaInitCalled1 = false;
    let onSchemaUpdateCalled1 = false;
    let onSchemaInitCalled2 = false;
    let onSchemaUpdateCalled2 = false;
    // upgradeManager start and uses 5 seconds to complete, upgradeManager2 start after 0.5 seconds,
    // so it should find the version table already created by upgradeManager and not call onSchemaInit,
    // but it should not fail because of the lock on the version table,
    // because upgradeManager should release it after creating the table
    const results = await Promise.allSettled([
      upgradeManager.checkDb(
        1,
        async (_dbClient: DbClient) => {
          onSchemaInitCalled1 = true;
          await setTimeout(5000);
        },
        async (_dbClient: DbClient, _fromVersion: number) => {
          onSchemaUpdateCalled1 = true;
        }
      ),
      async () => {
        await setTimeout(500);
        upgradeManager2.checkDb(
          1,
          async (_dbClient: DbClient) => {
            onSchemaInitCalled2 = true;
          },
          async (_dbClient: DbClient, _fromVersion: number) => {
            onSchemaUpdateCalled2 = true;
          }
        );
      }
    ]);
    // all proms must be fulfilled
    for (const result of results) {
      expect(result.status).toBe('fulfilled');
    }

    // only one between onSchemaInitCalled1 and onSchemaInitCalled2 must be true, the other must be false,
    // because only one of the two managers should have created the version table and called onSchemaInit

    expect(onSchemaInitCalled1).toBe(true);
    expect(onSchemaInitCalled2).toBe(false);
    expect(onSchemaUpdateCalled1).toBe(false);
    expect(onSchemaUpdateCalled2).toBe(false);
    const versionRow = await dbClient.get('SELECT value FROM _version_test3');
    expect(versionRow).toBeDefined();
    expect(versionRow!.value).toBe(1);
  });
});
