// thumbnail-worker.js (place this in the 
// server directory)
const sharp = require('sharp'); const { 
parentPort } = require('worker_threads'); 
// or use process.send if you prefer fork 
+ IPC parentPort.on('message', async 
(payload) => {
  const { input, options, outputPath, task 
  } = payload;
  try { let pipeline = sharp(input, { 
      failOn: options.processInvalidImages 
      ? 'none' : 'error', 
      limitInputPixels: false, raw: 
      options.raw, unlimited: true,
    })
      .pipelineColorspace(options.colorspace 
      === 'srgb' ? 'srgb' : 'rgb16') 
      .withIccProfile(options.colorspace);
    // (add the rest of your orientation / 
    // edits / resize logic here if 
    // needed)
    if (task === 'thumbnail') { await 
      pipeline
        .toFormat(options.format, { 
        quality: options.quality, 
        chromaSubsampling: options.quality 
        >= 80 ? '4:4:4' : '4:2:0' })
        .toFile(outputPath);
    } else if (task === 'thumbhash') {
      // thumbhash logic here
    }
    parentPort.postMessage({ success: true 
    });
  } catch (err) {
    parentPort.postMessage({ success: 
    false, error: err.message });
  }
});
