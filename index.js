const mqtt = require('mqtt');
let pi, localStorage;
let brokerAddr, myTopic, qos;

let mclient;

module.exports = {
    init: init,
    onCall: (method, path, args, transport)=>{
	if( method == 'GET' && path == 'testProcedure' )
	    return {success:true,message:'This is a test procedure in mqtt plugin'};
	return {};
    },
    //onUIGetSettings: function onUIGetSettings(settings){},
    //onUIGetSettingsSchema: function onUIGetSettingsSchema(schema, settings){},
    onUISetSettings: setupServerConnection,
};

/**
 * Initialize plugin
 * @param {object} _pluginInterface The interface of picogw plugin
 */
async function init(_pluginInterface) {
    pi = _pluginInterface;
    localStorage = pi.localStorage;
    /*await*/ setupServerConnection(pi.setting.getSettings()) ;


    // Publish test
    setInterval(()=>{
	pi.server.publish('testTopic',{message:'This is a test publish message from MQTT plugin'});
    },5000);
};

function setupServerConnection(settings){
    return new Promise((ac,rj)=>{
	if( settings.enabled ){

	    async function connectToMQTTBroker(){
		brokerAddr = settings.broker_address ;
		qos = settings.qos ;
		myTopic = await getMyMac(); // Mac address = topic to subscribe

		pi.log('Connecting to '+brokerAddr);

		// Allows self-signed certificate
		mclient  = mqtt.connect(brokerAddr,{rejectUnauthorized: false});
		
		mclient.on('connect', ()=>{
		    pi.log(`Connected to ${brokerAddr}. Waiting at topic '${myTopic}-Recv'.`);
		    mclient.subscribe(`${myTopic}-Req`, {qos:qos}, function (err,granted) {
			if( err ){
			    pi.log(err);
			    rj( {errors:[{message:`Could not subscribe to ${brokerAddr}/${myTopic}.`,error:err}]} );
			} else {
			    // pi.log(JSON.stringify(granted,null,'\t'));
			    ac(settings);
			}
		    });
		});
		
		mclient.on('message', (topic,message)=>{
		    if( topic != `${myTopic}-Req` ) return ;
		    try {
			message = JSON.parse(message);
			onRequest(message);
		    } catch(e){
			publish({errors:[{message:'Non-json request received',error:e}]});
		    }
		});

		mclient.on('error', (error)=>{
		    pi.log(err);
		    if(mclient){
			pi.log('Disconnecting from broker..');
			mclient.end(true);
			mclient = null ;
		    }
		});
	    }
	    if( mclient != null ){
		mclient.end(true,connectToMQTTBroker);
	    } else connectToMQTTBroker();
	} else {
	    if( mclient != null ){
		mclient.end(true,()=>{
		    pi.log('Disconnected from the current broker.');
		    mclient = null ;
		    ac(settings);
		});
	    } else {
		ac(settings);
	    }
	}

    });
}

function publish(obj){
    mclient.publish(myTopic, JSON.stringify(obj),{qos:pi.setting.getSettings().qos});
}

function onRequest(req) {
    // message is Buffer
    //console.log('Req:'+JSON.stringify(req));
    switch( req.method.toUpperCase() ){
    case 'GET' :
	pi.client.callProc(req).then(re=>{
	    re.tid = req.tid;
	    publish(re);
	}).catch(e=>{
	    e.tid = req.tid;
	    publish(e);
	});
	break ;
    case 'SUB' :
	pi.client.subscribe(req.path,publish);

	// Subscribed message (Ack)
	publish({success: true, tid: req.tid});

	break ;
    case 'UNSUB' :
	pi.client.unsubscribeall(req.path);
	// Unsubscribed message (Ack)
	publish({success: true, tid: req.tid});
	break ;
    default:
	pi.log(`Unsupported method:${req.method}`);
	break ;
    }

};


/**
 * Return my MAC address
 * @return {Promise} Promise object to query your Mac address
 */
function getMyMac() {
    let storedMac = localStorage.getItem('MAC_ADDRESS');
    if (storedMac != null) {
        return Promise.resolve(storedMac);
    } else {
        return new Promise((ac, rj)=>{
            const params = {method: 'GET', path: '/v1/admin/net'};
            pi.client.callProc(params).then((macs)=>{
                for (let mac in macs) {
                    if (macs[mac].self === true) {
                        localStorage.setItem('MAC_ADDRESS', mac);
                        ac(mac);
                        return;
                    }
                }
                rj({error: 'No localhost mac in db.'});
            }).catch((e)=>{
                rj({error: 'API call /admin/net/ failed.'});
            });
        });
    }
};

