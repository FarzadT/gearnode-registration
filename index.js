var Gearnode = require("gearnode");

var fn_void = function(){};
var log_void = {info:fn_void,error:fn_void,debug:fn_void}

///////////////////////////////////
// Forward declaration
var jobHandler = function(func,log) {

  return function (payload, job) {
    log.info(func.name + ':: is starting');

    try {
      func(payload, function (err, res) {
        log.info(func.name + ':: has returned');
        log.info('*****************************');

        if (err){
          res ={
            status:'error',
            reason:err.message
          }
        }

        job.complete(JSON.stringify(res));
      }, function(err,res){
        // this function will update the status
        log.info(func.name + ':: has has a status update');
        log.info('*****************************');

        if (err){
          res ={
            status:'error',
            reason:err
          };
          log.error(func.name + 'has error' + err);

          job.error(JSON.stringify(res));
          return;
        }

        job.data(JSON.stringify(res));
      });
    }
    catch (err) {
      log.info(func.name + ':: has an exception -- ' + err);
      job.error(err);
    }
  };
};

var setupWorker = function (tuGearnode, cb) {

  var worker;
  var log     = tuGearnode.log;
  var server  = tuGearnode.server;


  function create_worker() {

    var w = new Gearnode();
    w.addServer(tuGearnode.server);
    w.setWorkerId(tuGearnode.name);

    return w;

  }

  function onDisconnect(server) {

    log.error("Connection lost from " + server);

    function reconnect_job_server() {

      var connected = false;
      try {
        connected = worker.servers[server].connection.connected;
      }
      catch (err) { // just fall through
      }

      if (!connected) {

        log.error("creating worker.");

        worker = create_worker();
        // and schedule to check in 5 seconds. indefinitely...
        setTimeout(reconnect_job_server, 5000);
      }
      else {
        worker.on("disconnect", onDisconnect);

        tuGearnode.worker = worker;
        tuGearnode.setHandlers(tuGearnode.handlers);
      }
    }

    // try to reconnect
    reconnect_job_server();
  }

  worker = create_worker();

  worker.getExceptions(function (err, success) {
    if (!success) {
      log.error('this is the err' + err);
    }
  });
  worker.on("disconnect", onDisconnect);

  cb(null,worker);
};

// Our exported class
TUGearnode = function (name, serverIP, logger, cb){

  // Logger should support
  // debug
  // info
  // error
  cb = cb || fn_void;

  this.log = logger || log_void;
  this.name = name;
  this.server = serverIP;

  ///set up the gearnode worker
  var me = this;
  setupWorker(this, function(err, worker){

    me.worker = worker;

    cb(err, worker);

  });

  return this;
};

 TUGearnode.prototype.setHandlers = function (handlers) {

   var me = this;
  handlers.forEach(function (item) {

    item.handler.name = item.name;

    me.worker.addFunction(item.name,
      "utf-8",
      jobHandler(item.handler,me.log));
  });

  // store the handlers incase we need to reset them later
  me.handlers = handlers;

  return 0;
};


// Our exported class
module.exports = function (name, serverIP, logger, cb){
  return new TUGearnode(name, serverIP, logger, cb);
};
