#!/usr/local/bin/node

var util = require('util');
var fwk = require('pipes');
var cellar = require('cellar');
var crypto = require('crypto');

var cfg = require("./config.js");

/**
 * The Cohort Object
 * 
 * @extends {}
 * 
 * @param spec {port}
 */  
var cohort = function(spec, my) {
    my = my || {};
    var _super = {};
    
    my.sessionExpiry = 60 * 1000;   
    my.updateExpiry = 5 * 1000;
    my.updateFrequency = 4 * 1000;
    my.writebackFrequency = 31 * 1000;    

    fwk.populateConfig(cfg.config);  
    my.cfg = cfg.config;
    my.logger = fwk.logger();
    
    my.pipe = require('pipes').pipe({});
    my.mongo = cellar.mongo({ dbname: my.cfg['COHORT_DBNAME'] });

    my.sessions = {};
    my.curid = 0;
    my.hash = 'bootstrap';

    my.waiters = [];
    
    var that = {};
    
    var usage, main, send, forward;
    var uhash, update, writeback;
    var capture, getlive, getday, getcounter;
    
    usage = function() {
	console.log('Usage: cohort <pipesreg>');
	console.log('');
	console.log('<pipesreg> is the tag used to subscribe');
	console.log('to pipe messages for cohort');
	console.log('');
	console.log('Config values can be specified in the ENV or');
	console.log('on the command line using:');
	console.log('  cohort <pipesreg> --KEY=VALUE');
	console.log('');
    };

    main = function() {    
	var args = fwk.extractArgvs();
	args = args.slice(2);
	var ctx = fwk.context({ logger: my.logger,
				config: my.cfg });
	
	if(args.length != 1) { usage(); return; }

	my.mongo.get(
	    ctx, 'sessions.bootstrap',      
	    function(obj) {	
		my.nextid = obj.nextid || 1;
		
		my.pipe.subscribe(args[0], 'cohort');
		my.pipe.on('1w', forward);
		my.pipe.on('2w', forward);
		
		setInterval(update, my.updateFrequency);
		setInterval(writeback, my.writebackFrequency);
	    });    
    };


    send = function(ctx, reply) {
	my.pipe.send(reply, function(err, hdr, res) {
			 if(err)
			     ctx.log.error(err);
			 /** TODO push an error message */
		     });    
	ctx.finalize();
    };
    
    forward = function(id, msg) {
	var ctx = fwk.context({ logger: my.logger,
				config: my.cfg });
	
	/** error handling */
	ctx.on('error', function(err) {
		   if(msg.type() === '2w') {
		       var reply = fwk.message.reply(msg);
		       reply.setBody({ error: err.message });
		       send(ctx, reply);
		   }
		   /** else nothing to do */
		   /** TODO push an error mesage */		  
	       });

	try {      
	    switch(msg.subject() + '-' + msg.type()) {
		
	    case 'COH:CAPTURE-1w':
	    case 'COH:CAPTURE-2w':
		ctx.log.out(msg.toString());
		capture(ctx, msg, function(res) {
			    if(msg.type() === '2w') {
				var reply = fwk.message.reply(msg);
				reply.setBody(res);
				send(ctx, reply);
			    }
			});
		break;

	    case 'COH:GETLIVE-2w':
		ctx.log.out(msg.toString());
		getlive(ctx, msg, function(res) {
			    if(msg.type() === '2w') {
				var reply = fwk.message.reply(msg);
				reply.setBody(res);
				send(ctx, reply);
			    }
			});
		break;
	    case 'COH:GETDAY-2w':
		ctx.log.out(msg.toString());
		getday(ctx, msg, function(res) {
			   if(msg.type() === '2w') {
			       var reply = fwk.message.reply(msg);
			       reply.setBody(res);
			       send(ctx, reply);
			   }
		       });
		break;
	    case 'COH:GETCOUNTER-2w':
		ctx.log.out(msg.toString());
		getcounter(ctx, msg, function(res) {
			       if(msg.type() === '2w') {
				   var reply = fwk.message.reply(msg);
				   reply.setBody(res);
				   send(ctx, reply);
			       }
		       });
		break;

	    default:
		ctx.error(new Error('ignored: ' + msg.toString()));
		break;
	    }
	}
	catch(err) {
	    ctx.error(err, true);      
	}
    };

    writeback = function() {	
	var ctx = fwk.context({ logger: my.logger,
				config: my.cfg });
	
	/** update sessions.bootstrap */
	my.mongo.get(
	    ctx, 'sessions.bootstrap',
	    function(obj) {
		obj.nextid = my.nextid;
		my.mongo.set(ctx, 
			     'sessions.bootstrap',
			     obj._hash,
			     obj,
			     function(status) {});
	    });
	
	var s = my.sessions;
	my.sessions = {};
	var e = {};

	/** update sessions.*** */
	fwk.forEach(s, function(session, user) {
			/** 60s inactivity */
			if(((new Date()).getTime() - session.end.getTime()) > my.sessionExpiry) {
			    session.end = new Date();
			    e[user] = session;
			}
			else {
			    my.sessions[user] = session;
			}
		    });
	
	fwk.forEach(e, function(session, user) {
			uhash('removed');
			my.mongo.get(
			    ctx, 'sessions.' + session.id,
			    function(obj) {
				my.mongo.set(ctx, 
					     'sessions.' + session.id,
					     obj._hash,
					     session,
					     function(status) {
						 ctx.log.debug('WRITEBACK (exp): [' + session.id + '] ' + user + ' - ' + status);
					     });
			    });
		    });
	
	fwk.forEach(my.sessions, function(session, user) {
			my.mongo.get(
			    ctx, 'sessions.' + session.id,
			    function(obj) {
				my.mongo.set(ctx, 
					     'sessions.' + session.id,
					     obj._hash,
					     session,
					     function(status) {
						 ctx.log.debug('WRITEBACK: [' + session.id + '] ' + user + ' - ' + status);
					     });
			    });			  			  
		    });
    };

    update = function() {
	var w = my.waiters;
	my.waiters = [];

	for(var i = 0; i < w.length; i ++) {
	    if(w[i].hash !== my.hash) {
		w[i].cb({ hash: my.hash,
			  sessions: my.sessions });
	    } 
	    else {
		if(((new Date).getTime() - w[i].date.getTime()) > my.updateExpiry) {
		    w[i].cb({ hash: my.hash });	  
		}
		else {
		    my.waiters.push(w[i]);	  
		}
	    }      
	}
    };
    
    uhash = function(nhash) {
	var hash = crypto.createHash('sha1');
	hash.update(my.hash);
	hash.update(nhash);
	my.hash = hash.digest(encoding='hex');
    };

    capture = function(ctx, msg, cb_)  {
	if(!msg.body())
	{ ctx.error(new Error('Invalid req: empty body')); return; }
	
	if(!Array.isArray(msg.targets()) || msg.targets().length != 1)
	{ ctx.error(new Error('Invalid req: targets must be length one array')); return; }
	
	if(!msg.body().action)
	{ ctx.error(new Error('Invalid req: action missing')); return; }
	
	var user = msg.targets()[0];
	
	if(!my.sessions.hasOwnProperty(user)) {
	    my.sessions[user] = { user: user,			 
				  start: new Date(),
				  id: my.nextid,
				  log: [] 
				};
	    my.nextid += 1;
	}
	
	my.sessions[user].end = new Date();
	
	var item = { action: msg.body().action,
		     date: my.sessions[user].end,
		     data: msg.body().data		 
		   };

	if(msg.body().loc && 
	   Array.isArray(action.body().loc) && 
	   action.body().loc.length === 2) {
	    item.loc = msg.body().loc;
	}
	if(msg.body().loctype) {
	    item.loctype = msg.body().loctype;
	}    
	
	my.sessions[user].log.push(item);    

	/** update counters */
	var dstr = item.date.getDate() + 'd' + 
	    item.date.getMonth() + 'm' + 
	    item.date.getFullYear() + 'y' ;
	my.mongo.get(
	    ctx, 'counters.' + dstr,
	    function(obj) {
		if(typeof obj.data === 'undefined')
		    obj.data = {};
		if(typeof obj.data[item.action] === 'undefined')
		    obj.data[item.action] = 0;

		obj.data[item.action] += 1;				
		my.mongo.set(ctx, 
			     'counters.' + dstr,
			     obj._hash,
			     obj,
			     function(status) {
				 ctx.log.debug('INC COUNTER: [' + dstr + '] ' + user + '.' + item.action + ' - ' + status);
			     });
	    });
	var mstr = item.date.getMonth() + 'm' + 
	    item.date.getFullYear() + 'y';	
	my.mongo.get(
	    ctx, 'counters.' + mstr,
	    function(obj) {
		if(typeof obj.data === 'undefined')
		    obj.data = {};
		if(typeof obj.data[item.action] === 'undefined')
		    obj.data[item.action] = 0;
		
		obj.data[item.action] += 1;				
		my.mongo.set(ctx, 
			     'counters.' + mstr,
			     obj._hash,
			     obj,
			     function(status) {
				 ctx.log.debug('INC COUNTER: [' + mstr + '] ' + user + '.' + item.action + ' - ' + status);
			     });
	    });
	var ystr = item.date.getFullYear() + 'y';	
	my.mongo.get(
	    ctx, 'counters.' + ystr,
	    function(obj) {
		if(typeof obj.data === 'undefined')
		    obj.data = {};
		if(typeof obj.data[item.action] === 'undefined')
		    obj.data[item.action] = 0;
		
		obj.data[item.action] += 1;				
		my.mongo.set(ctx, 
			     'counters.' + ystr,
			     obj._hash,
			     obj,
			     function(status) {
				 ctx.log.debug('INC COUNTER: [' + ystr + '] ' + user + '.' + item.action + ' - ' + status);
			     });
	    });
	
	
	uhash(fwk.makehash(item));
	update();

	cb_({status: 'DONE'});
    };
    
    getlive = function(ctx, msg, cb_) {	
	var hash = '';
	if(msg.body() && msg.body().hash) {
	    hash = msg.body().hash;
	}
	
	my.waiters.push({ hash: hash,
			  cb: cb_,
			  date: new Date() });    
	update();
    };
    
    getday = function(ctx, msg, cb_)  {
	if(!msg.body())
	{ ctx.error(new Error('Invalid req: empty body')); return; }
        
	if(typeof msg.body().day === 'undefined')
	{ ctx.error(new Error('Invalid req: day missing')); return; }    
	if(typeof msg.body().month === 'undefined')
	{ ctx.error(new Error('Invalid req: month missing')); return; }    
	if(typeof msg.body().year === 'undefined')
	{ ctx.error(new Error('Invalid req: year missing')); return; }

	var day = parseInt(msg.body().day, 10);
	var month = parseInt(msg.body().month, 10);
	var year = parseInt(msg.body().year, 10);
	
	var beg = new Date(year, month, day);
	var end = new Date(beg.getTime() + (1000 * 60 * 60 * 24));

	my.mongo.find(ctx, 
		      'sessions.bootstrap', 
		      { 'start': { '$gt': beg, '$lte': end } },
		      function(result) {
			  cb_({ sessions: result });
		      });
    };

    getcounter = function(ctx, msg, cb_) {
	if(!msg.body())
	{ ctx.error(new Error('Invalid req: empty body')); return; }
        
	if(typeof msg.body().day === 'undefined')
	{ ctx.error(new Error('Invalid req: day missing')); return; }    
	if(typeof msg.body().month === 'undefined')
	{ ctx.error(new Error('Invalid req: month missing')); return; }    
	if(typeof msg.body().year === 'undefined')
	{ ctx.error(new Error('Invalid req: year missing')); return; }	  

	var day = parseInt(msg.body().day, 10);
	var month = parseInt(msg.body().month, 10);
	var year = parseInt(msg.body().year, 10);
	

	/** update counters */	
	var mplex = fwk.mplex({});  
	var res = {};

	(function(cb) {
	     var dstr = day + 'd' + 
		 month + 'm' + 
		 year + 'y' ;
	     my.mongo.get(
		 ctx, 'counters.' + dstr,
		 function(obj) {	
		     res.day = obj.data;
		     cb('done');
		 });	     
	 })(mplex.callback());

	(function(cb) {
	     var mstr = month + 'm' + 
		 year + 'y';	
	     my.mongo.get(
		 ctx, 'counters.' + mstr,
		 function(obj) {	
		     res.month = obj.data;		     
		     cb('done');
		 });	     
	 })(mplex.callback());

	(function(cb) {
	     var ystr = year + 'y';	
	     my.mongo.get(
		 ctx, 'counters.' + ystr,
		 function(obj) {	
		     res.year = obj.data;		     
		     cb('done');
		 });	     
	 })(mplex.callback());

	mplex.go(function() {	     	     
		     cb_(res);
		 });	
    };
    
    fwk.method(that, 'main', main);

    return that;  
};

/** main */
cohort({}).main();
