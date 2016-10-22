/*******************************************************************************
 * Copyright [2016] [Dhanuka Ranasinghe]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

package org.apache.kafka.connect.socket.batch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;

public class RecordBatch implements Serializable{

	private static final long serialVersionUID = 8231175871739109470L;
	
	private final List<SourceRecord> requests;
	private long lastAttemptMs;

	public RecordBatch(long now) {
		this.lastAttemptMs = now;
		this.requests = new ArrayList<>();
	}

	public void add(SourceRecord request) {
		requests.add(request);
	}

	public List<SourceRecord> requests() {
		return requests;
	}

	public long getLastAttemptMs() {
		return lastAttemptMs;
	}

}
