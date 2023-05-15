const { SlippiGame } = require("@slippi/slippi-js"); 

// Note to self: This is how javascript takes arguments, the two commas are used to skip over index 0 and 1 of the arguments read, which are the node.js executable path and JS filepath respectively
	
const [ , , filepath ] = process.argv;

const game = new SlippiGame(filepath)
// Get game settings – stage, characters, etc
const settings = game.getSettings();
console.log(settings);

// Get metadata - start time, platform played on, etc
const metadata = game.getMetadata();
console.log(metadata);

// Get computed stats - openings / kill, conversions, etc
const stats = game.getStats();
console.log(stats);

// Get frames – animation state, inputs, etc
// This is used to compute your own stats or get more frame-specific info (advanced)
const frames = game.getFrames();
console.log(frames[0].players); // Print frame when timer starts counting down
