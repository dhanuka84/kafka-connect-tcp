package org.apache.kafka.connect.socket.database;

import org.apache.kafka.connect.socket.database.AbstractDBManager.DBType;

public class ElasticSearchDAO extends AbstractDAO{

	private ElasticSearchDBManager manager = (ElasticSearchDBManager) AbstractDBManager.getDBManager(DBType.ELASTICSEARCH);
	
	@Override
	public AbstractDBManager getDBManager() {
		return manager;
	}
	
	public void updateIndexData(){
		//manager.getClient().execute(clientRequest);
	}
	
	/* 

	    def update_index_data(self, index, doc_type, id, body):
	        try:
	            self.es.update(index=index, doc_type=doc_type, id=id, body=body)
	            self.logger.info('action: update elasticsearch | index: %s | id: %s | status: successful', index, id)
	            return True
	        except Exception, e:
	            self.logger.error('action: update elasticsearch | index: %s | id: %s | status: unsuccessful', index, id)
	            self.logger.exception(e)

	    def get_event_by_state_trigger_id(self, hash_value):
	        return self.read_index_data(index=LIVE_ALERT_STATES, doc_type=LIVE_ALERT_STATES, id=hash_value)

	    def insert_event(self, data_dict):
	        self.create_index_data(
	            index=LIVE_ALERT_STATES, doc_type=LIVE_ALERT_STATES, id=data_dict['stateTriggerId'], body=data_dict
	        )

	    def update_event(self, data_dict, event_from_db):
	        self.update_index_data(
	            index=LIVE_ALERT_STATES, doc_type=LIVE_ALERT_STATES, id=data_dict['stateTriggerId'], body={
	                "doc": {
	                    "timestampUpdated": data_dict['timestampUpdated'],
	                    "severity": data_dict['severity'],
	                    "title": data_dict['title'],
	                    "trigger": data_dict['trigger'],
	                    "sourceEventsCount": int(event_from_db.get('sourceEventsCount', 0)) + 1
	                }
	            })

	    def reset_event(self, data_dict):
	        try:
	            # date_time = datetime.now().strftime('%Y.%m.%d')
	            try:
	                date_time_of_index = data_dict['closedTimestamp'].split('T')[0].replace('-', '.')
	            except:
	                date_time_of_index = datetime.now().strftime('%Y.%m.%d')

	            print date_time_of_index

	            actions = [
	                {
	                    '_op_type': 'delete',
	                    '_index': LIVE_ALERT_STATES,
	                    '_type': LIVE_ALERT_STATES,
	                    '_id': data_dict['stateTriggerId'],
	                },
	                {
	                    '_op_type': 'create',
	                    '_index': '%s-%s' % (HISTORICAL_ALERT_STATES, date_time_of_index),
	                    '_type': HISTORICAL_ALERT_STATES,
	                    '_id': data_dict['objectId'],
	                    '_source': data_dict
	                }
	            ]

	            self.logger.info('action: bulk update elasticsearch | actions: %s', actions)

	            errors = helpers.bulk(self.es, actions)
	            print errors
	        except Exception, e:
	            self.logger.exception(e)*/
	
	

	
	
	

}
