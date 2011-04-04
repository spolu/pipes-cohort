#!/usr/local/bin/node

var util = require('util');
var fwk = require('fwk');
var mongo = require('mongo');

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
  
  fwk.populateConfig(cfg.config);  
  my.cfg = cfg.config;
  my.logger = fwk.logger();
  
  my.pipe = {};
  my.mongo = mongo.mongo({ dbname: 'cohort' });

  my.sessions = {};
  my.curid = 0;
  my.hash = 'bootstrap';

  my.waiters = [];
  
  var that = {};
  
  var usage, main, send, forward;
  var uhash, update, writeback;
  var capture, getlive, getday, cleanup;
  
  usage = function() {
    console.log('Usage: cohort <pipereg>');
    console.log('');
    console.log('<pipereg> is the tag used to subscribe');
    console.log('to pipe messages for cohort');
    console.log('');
    console.log('Config values can be specified in the ENV or');
    console.log('on the command line using:');
    console.log('  cohort <pipereg> --KEY=VALUE');
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
	
	setInterval(update, 5000);
	setInterval(writeback, 30000);
      });    
  };


  send = function(pipe, ctx, reply) {
    pipe.send(reply, function(err, hdr, res) {
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
    s.forEach(function(session, user)) {
      session.end = new Date();
      if((session.end.getTime() - session.start.getTime()) > 60 * 1000) {
	e[user] = session;
      }
      else {
	my.sessions[user] = session;
      }
    }
    
    e.forEach(function(session, user) {
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
    
    my.sessions.forEach(function(session, user) {
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
	w[i].cb(my.sessions);
      } 
      else {
	if(((new Date).getTime() - w[i].date.getTime()) > 5000) {
	  w[i].cb({});	  
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
    
    uhash(item.makehash());
    update();

    cb_({status: 'DONE'});
  };
  
  getlive = function(ctx, msg, cb_) {
    
    var hash = '';
    if(msg.body() && msg.body().hash) {
      hash = msg.body().hash;
    }
    
    my.waiters.push({ cb: cb_,
		      date: new Date() });    
    update();
  };
  
  getday = function(ctx, msg, cb_)  {

    if(!msg.body())
    { ctx.error(new Error('Invalid req: empty body')); return; }
        
    if(!msg.body().day)
    { ctx.error(new Error('Invalid req: day missing')); return; }    
    if(!msg.body().month)
    { ctx.error(new Error('Invalid req: month missing')); return; }    
    if(!msg.body().year)
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
		    cb_(result);
		  });
  };
  
  that.method('main', main);

  return that;  
};

/** main */
cohort({}).main();
