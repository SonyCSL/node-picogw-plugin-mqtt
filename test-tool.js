const brokerAddr = 'mqtt:test.mosquitto.org';
const myTopic = '00:00:00:00:00:00';
const qos = 1 ;


// MQTT API call examples.
function onConnect(){
    setTimeout(()=>{

	// Procedure call example
	callProc({method:'GET',path:'/v1/mqtt/testProcedure'})
	    .then(msg=>{
		console.log('callProc reply:');
		console.log(JSON.stringify(msg,null,'\t'));
	    })
	    .catch(console.error);


	// PubSub (Subscribe) example
	subscribe('/v1/mqtt/testTopic',(pubVal)=>{
	    console.log('Published:'+JSON.stringify(pubVal,null,'\t'));
	}).then(re=>{
	    console.log('Subscribed:'+JSON.stringify(re));
	});

	setTimeout(()=>{
	    console.log('Unsubscribe topic.');
	    unsubscribe('/v1/mqtt/testTopic').then(re=>{
		console.log('Unsubscribed:'+JSON.stringify(re));
	    });
	},10*1000); // unsubscribe after 10 secs

    },2000);
}




// Defines timeout when no reply is got from the broker
const timeout_time = 60*1000 ;

// Request topic:                     `${myTopic}-Req`
// Reply receive topic (Check tid!):  `${myTopic}`

const mqtt = require('mqtt');

// Connect to the broker allowing self-signed certificate
const mclient = mqtt.connect(brokerAddr,{rejectUnauthorized: false});


mclient.on('connect', ()=>{
    // Connected callback
    console.log(`Connected to ${brokerAddr}. Waiting at topic '${myTopic}'.`);
    // A topic to receive reply from PicoGW
    mclient.subscribe(myTopic, {qos:qos}, function (err,granted) {
	if( err ){
	    // Not connected
	    console.error(err);
	} else {
	    // Connected.
	    // console.log(JSON.stringify(granted,null,'\t'));
	    onConnect();
	}
    });
});

// Reply from PicoGW
mclient.on('message', (topic,message)=>{
    // console.log(`OnMessage:${topic}/${message}`);
    if( topic != myTopic ) return ; // Message from uninterested topic.
    try {
	// Convert string to JSON object
	message = JSON.parse(message);

	// Message receive handler
	onRecv(message);
    } catch(e){
	// Non-JSON reply
	console.error(e);
	return;
    }
});

// Error handler
mclient.on('error', console.error);



// callReqs stores transactions that are waiting for reply
// key: transaction id, value: promise
let callReqs = {};

// subs stores active subscriptions
// key: path, value: callback functions
let subs = {};

// next transaction id.
let tidMax = 0 ;

// Message receive handler
function onRecv(obj){
    switch( obj.method ){
    case 'PUB': // Published from PicoGW
	// list paths
	for( let path in obj ){
	    if( path.slice(-1)=='/') path = path.slice(0,-1);
	    if( path.indexOf('/v1/') != 0 || subs[path] == null ) continue ;
	    // Call subscribed handlers
	    subs[path].forEach(handler=>{
		handler(obj[path]);
	    });
	}
	break;
    default : // Assuming callProc reply or subscribed/unsubscribed
	const tid = obj.tid;
	delete obj.tid;
	if( tid == null || callReqs[tid] == null ){
	    console.error('Unhandled message: '+JSON.stringify(obj));
	    return ;
	}
	const [ac,rj] = callReqs[tid]; delete callReqs[tid];
	// resolve by the received object.
	ac(obj);
	break;
    } 
}

// timeout definition when reply is not sent by the broker
function defineTimeout(tid){
    setTimeout(()=>{
	if( callReqs[tid] == null ) return ;
	const [ac,rj] = callReqs[tid];
	delete callReqs[tid];
	rj({errors:[{message:'Timeout'}]});
	delete callReqs[tid];
    },timeout_time );
}

/////////////////////////////////
// Procedure call method
function callProc(obj){
    return new Promise((ac,rj)=>{
	const tid = ++tidMax;
	obj.tid = tid ;
	// Stores promises into callReqs
	callReqs[tid] = [ac,rj];
	// Send callProc request to PicoGW
	mclient.publish(`${myTopic}-Req`,JSON.stringify(obj),{qos:qos});
	defineTimeout(tid);
    });
}

/////////////////////////////////
// Subscribe/Unsubscribe
// callback can be called multiple times
function subscribe(path,callback){
    // Normalize path (=topic)
    if( path.slice(-1)=='/') path = path.slice(0,-1);
    path = path.toLowerCase();
    return new Promise((ac,rj)=>{
	if( !(subs[path] instanceof Array) ){
	    // First time subscription to path
	    subs[path] = [];
	} else if( subs[path].some(f=>(f==callback)) ){
	    // Already subscribed topic
	    console.log('Duplicated subscription');
	    ac();
	    return ;
	}
	// Store publish callback into subs object
	subs[path].push(callback);

	// Wait for 'Subscribed' message
	const tid = ++tidMax;
	callReqs[tid] = [ac,rj];

	// Send subscription request to PicoGW
	mclient.publish(`${myTopic}-Req`
			,JSON.stringify({method:'SUB',path:path,tid:tid})
			,{qos:qos});
	defineTimeout(tid);

    });
}

function unsubscribe(path){
    if( path.slice(-1)=='/') path = path.slice(0,-1);
    path = path.toLowerCase();

    return new Promise( (ac,rj)=>{

	// Delete publish handler
	delete subs[path];

	// Wait for 'Unsubscribed' message
	const tid = ++tidMax;
	callReqs[tid] = [ac,rj];

	// Send request
	mclient.publish(
	    `${myTopic}-Req`
	    ,JSON.stringify({method:'UNSUB',path:path,tid:tid})
	    ,{qos:qos});
	defineTimeout(tid);
    } );
}
