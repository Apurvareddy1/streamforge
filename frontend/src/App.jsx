import { useCallback } from "react";
import {
  ReactFlow,
  MiniMap,
  Controls,
  Background,
  addEdge,
  useNodesState,
  useEdgesState,
} from "@xyflow/react";

import "@xyflow/react/dist/style.css";

const initialNodes = [
  {
    id: "1",
    position: { x: 50, y: 150 },
    data: { label: "Kafka Topic\nTruck Telemetry" },
  },
  {
    id: "2",
    position: { x: 300, y: 150 },
    data: { label: "Python Producer" },
  },
  {
    id: "3",
    position: { x: 550, y: 150 },
    data: { label: "Kafka Stream" },
  },
];

const initialEdges = [
  {
    id: "e1-2",
    source: "1",
    target: "2",
    animated: true,
  },
  {
    id: "e2-3",
    source: "2",
    target: "3",
    animated: true,
  },
];

function App() {
  const [nodes, setNodes, onNodesChange] =
    useNodesState(initialNodes);

  const [edges, setEdges, onEdgesChange] =
    useEdgesState(initialEdges);

  const onConnect = useCallback(
    (connection) =>
      setEdges((currentEdges) =>
        addEdge(connection, currentEdges)
      ),
    [setEdges]
  );

  return (
    <div style={{ width: "100vw", height: "100vh" }}>
      
      <div
        style={{
          height: "70px",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          fontSize: "26px",
          fontWeight: "bold",
          borderBottom: "1px solid #ddd",
        }}
      >
        StreamForge - Distributed Event Processor
      </div>

      <div style={{ height: "calc(100vh - 70px)" }}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          fitView
        >
          <Controls />
          <MiniMap />
          <Background />
        </ReactFlow>
      </div>
    </div>
  );
}

export default App;