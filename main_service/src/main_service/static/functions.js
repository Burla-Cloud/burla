let clusterIsOn = false; // Track cluster status globally
let eventSource; // Reuse the EventSource globally

function watchCluster() {
    const nodesElement = document.getElementById('monitor-message');
    const clusterElement = document.getElementById('cluster-status');
    const restartButton = document.querySelector('button');
    let eventSource = new EventSource('/v1/cluster');
    let nodes = {};

    clusterElement.textContent = "OFF";
    restartButton.textContent = "Start Cluster";

    eventSource.onmessage = function(event) {
        const data = JSON.parse(event.data);
        const { nodeId, status, deleted } = data;

        if (status) {
            nodes[nodeId] = status;
        } else if (deleted) {
            delete nodes[nodeId];
        }
        
        updateNodesStatus(nodes);
        updateClusterStatus(nodes, restartButton);
    };

    eventSource.onerror = function(error) {
        nodesElement.innerHTML = "";
        nodesElement.textContent = "Error: Unable to receive updates.";
        eventSource.close();
    };

    function updateNodesStatus(nodes) {
        nodesElement.innerHTML = "";

        for (const nodeId in nodes) {
            const status = nodes[nodeId];
            const nodeElement = document.createElement("div");
            nodeElement.textContent = `Node ${nodeId} is ${status}`;
            nodesElement.appendChild(nodeElement);
        }
    }

    function updateClusterStatus(nodes, restartButton) {
        const nodeStatuses = Object.values(nodes);

        if (nodeStatuses.length === 0) {
            clusterElement.textContent = "OFF";
            restartButton.textContent = "Start Cluster";

        } else if (nodeStatuses.includes("BOOTING")) {
            clusterElement.textContent = "BOOTING";
            restartButton.textContent = "Restart Cluster";

        } else if (nodeStatuses.every(status => status === "READY")) {
            clusterElement.textContent = "ON";
            restartButton.textContent = "Restart Cluster";
        }
    }
}

function startCluster() {
    const restartButton = document.querySelector('button');
    const messageElement = document.getElementById('response-message');

    // Clear any previous message or loader
    messageElement.textContent = '';
    
    // Reset the message color to default (black or your preferred color)
    messageElement.style.color = "black";

    // Display the "Cluster starting up" message and the loader
    const loaderMessage = document.createElement('span');
    loaderMessage.textContent = 'Loading';
    const loader = document.createElement('span');
    loader.className = 'loader';
    loader.textContent = ' / '; // Initial loader symbol

    // Append the loader to the message element
    messageElement.appendChild(loaderMessage);
    messageElement.appendChild(loader);

    // Animate the loader
    const symbols = ['/', '-', '\\', '|'];
    let index = 0;
    const intervalId = setInterval(() => {
        loader.textContent = symbols[index];
        index = (index + 1) % symbols.length;
    }, 140);

    const isLocalhost = window.location.hostname === 'localhost';
    const baseUrl = isLocalhost 
        ? 'http://localhost:5001'  // Development environment
        : 'https://cluster.burla.dev';  // Production environment

    // Call the cluster start API
    fetch(`${baseUrl}/v1/cluster/restart`, { method: 'POST' })
        .then(response => {
            if (response.ok) {
                // If the response is OK, do nothing further
                return;
            } else {
                return response.json().then(err => {
                    throw new Error(`Cluster Failed - ${response.status} ${response.statusText}`);
                });
            }
        })
        .catch(error => {
            // Display the error message if the response is not OK
            messageElement.textContent = `Error: ${error.message}`;
            messageElement.style.color = "red";
        })
        .finally(() => {
            // Add a 1-second delay before stopping the loader
            setTimeout(() => {
                clearInterval(intervalId);  // Stop the loader animation
                loader.remove();
                loaderMessage.remove();
                restartButton.disabled = false; // Re-enable the button
            }, 800);  // 1-second delay
        });
}

window.onload = function() {
    watchCluster();
};
