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

import { getDateAsUTCMysqlString } from '../src';

describe('utils test', () => {
  describe('getDateAsUTCMysqlString', () => {
    const testsA = [
      {
        in: '2023-11-24T05:02:01Z',
        out: '2023-11-24 05:02:01'
      },
      {
        in: '2023-11-24T07:02:01+02:00',
        out: '2023-11-24 05:02:01'
      },
      {
        in: '2023-11-24T05:02:01.001Z',
        out: '2023-11-24 05:02:01'
      },
      {
        in: '2023-11-24T07:02:01.001+02:00',
        out: '2023-11-24 05:02:01'
      }
    ];
    for (const test of testsA) {
      it(`should return correct string without ms (${test.in})`, () => {
        const s = getDateAsUTCMysqlString(new Date(test.in));
        expect(s).toBe(test.out);
      });
    }
    const testsB = [
      {
        in: '2023-11-24T05:02:01.001Z',
        out: '2023-11-24 05:02:01.001'
      },
      {
        in: '2023-11-24T05:02:01.100Z',
        out: '2023-11-24 05:02:01.100'
      },
      {
        in: '2023-11-24T07:02:01.000+02:00',
        out: '2023-11-24 05:02:01.000'
      }
    ];
    for (const test of testsB) {
      it(`should return correct string with ms (${test.in})`, () => {
        const s = getDateAsUTCMysqlString(new Date(test.in), true);
        expect(s).toBe(test.out);
      });
    }
  });
});
