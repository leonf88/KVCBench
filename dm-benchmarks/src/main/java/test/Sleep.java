/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package test;

import mpid.core.MPI_D;
import mpid.core.MPI_D_Exception;

import java.io.IOException;
import java.util.HashMap;

public class Sleep {
    public static void main(String[] args) throws IOException, InterruptedException {
        int sleepSecond = 10;
        if (args.length == 1) {
            sleepSecond = Integer.parseInt(args[0]);
        }
        try {
            HashMap<String, String> conf = new HashMap<String, String>();
            MPI_D.Init(args, MPI_D.Mode.Common, conf);
            if (MPI_D.COMM_BIPARTITE_O != null) {
                System.out.println("O task sleep " + sleepSecond + " sec.");
                Thread.sleep(1000 * sleepSecond);
            } else if (MPI_D.COMM_BIPARTITE_A != null) {
                System.out.println("A task sleep " + sleepSecond + " sec.");
                Thread.sleep(1000 * sleepSecond);
            }
            MPI_D.Finalize();
        } catch (MPI_D_Exception e) {
            e.printStackTrace();
        }
    }
}
