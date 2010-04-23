/*
 * Copyright 2010 Happy-Camper Street.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package st.happy_camper.hadoop.scala.aggregate

import _root_.java.util._

import _root_.org.apache.commons.lang.builder._

/**
 * @author ueshin
 */
class Access(val ip: String, val url: String, val accessDate: Date) extends Comparable[Access] {

  def compareTo(o: Access): Int = {
    new CompareToBuilder().append(ip, o.ip).append(url, o.url).append(accessDate, o.accessDate).toComparison
  }

}
