var spawn = require('child_process').spawn;
var exec = require('child_process').exec;
var execSync = require('child_process').execSync;
var fs = require('fs');
var numTest = parseInt(process.argv[2]);

try {
  execSync('rm *.txt');
}catch(e){
}

fs.writeFileSync('master.txt', '');
var files = [];
// True if failed. Keep failed only if option available.
var failedFiles = {};
var closed = 0;
for(var i=0;i<numTest;i++){
  var fileName = 'TEST'+ i.toString() + Math.random().toString(26)  + '.txt';
  files.push(fileName);
  (function(fileName){
    var test = spawn('go', ['test']);
    test.stdout.on('data', function(data){
      var stringed = String(data);
      if(stringed.indexOf('FAIL') >= 0){
        failedFiles[fileName] = true;
      }
      fs.appendFile(fileName, data);
    })
    test.stderr.on('data', function(data){
      var stringed = String(data);
      if(stringed.indexOf('FAIL') >= 0){
        failedFiles[fileName] = true;
      }
      if(stringed.indexOf('method') < 0 && stringed.indexOf('die') < 0){
        fs.appendFile(fileName, data);
      }
    })
    test.on('close', function(){
      closed ++;
      if(closed === numTest){
        combineFiles(files);
      }
    })
  })(fileName);
};

function combineFiles(files){
  files.forEach(function(el){
    if(failedFiles[el]){
      fs.readFile(el, function(err, data){
        if(!err){
          // Append to master logs.
          fs.appendFile('master.txt', data);
        }
      })
    }
  })
}

process.on('SIGINT', function(){
  execSync('rm *.txt');
})
