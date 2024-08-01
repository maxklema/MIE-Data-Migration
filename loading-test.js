const total = 100; // Total length of the progress bar

// Function to display the loading bar
function displayLoadingBar(percentage) {
  const progress = Math.round((percentage / 100) * total);
  const emptySpace = total - progress;
  
  // Create the loading bar
  const bar = `[${'='.repeat(progress)}${' '.repeat(emptySpace)}] ${percentage}%`;
  
  // Use '\r' to overwrite the line in the terminal
  process.stdout.write(`${bar}\r`);
}

// Simulate progress updates
let percentage = 0;
const interval = setInterval(() => {
  if (percentage > 100) {
    clearInterval(interval);
    process.stdout.write('Loading complete!\n');
  } else {
    displayLoadingBar(percentage);
    percentage += 5; // Increment percentage
  }
}, 100); // Update every 100 milliseconds
