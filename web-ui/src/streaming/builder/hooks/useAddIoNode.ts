import { useCallback } from 'react'
import { NodeProps, useReactFlow, getConnectedEdges } from 'reactflow'
import { AttachedConnector, Direction } from 'src/types/manager'
import { ConnectorDescr } from 'src/types/manager/models/ConnectorDescr'
import { randomString } from 'src/utils/randomString'

const HEIGHT_OFFSET = 120

export function useAddConnector() {
  const { setNodes, getNodes, getNode, addNodes, addEdges } = useReactFlow()

  const addNewConnector = useCallback(
    (connector: ConnectorDescr, ac: AttachedConnector) => {
      // Input or Output?
      const newNodeType = ac.direction === Direction.INPUT ? 'inputNode' : 'outputNode'
      const placeholderId = ac.direction === Direction.INPUT ? 'inputPlaceholder' : 'outputPlaceholder'
      const placeholder = getNode(placeholderId)
      if (!placeholder) {
        return
      }

      // If this node already exists, don't add it again
      const existingNode = getNodes().find(node => node.id === ac.uuid)
      if (existingNode) {
        return
      } else {
        // Move the placeholder node down a bit
        setNodes(nodes =>
          nodes.map(node => {
            if (node.id === placeholderId) {
              return {
                ...node,
                position: { x: placeholder.position.x, y: placeholder.position.y + HEIGHT_OFFSET }
              }
            }

            return node
          })
        )

        // Add the new nodes
        addNodes({
          position: { x: placeholder.position.x, y: placeholder.position.y },
          id: ac.uuid,
          type: newNodeType,
          deletable: true,
          data: { connector, ac }
        })
      }

      // Now that we have the node, we need to add a connector if we have one
      const sqlNode = getNode('sql')
      const ourNode = getNode(ac.uuid)
      const tableOrView = ac.config
      const sqlPrefix = ac.direction === Direction.INPUT ? 'table-' : 'view-'
      const connectorHandle = sqlPrefix + tableOrView
      const hasAnEdge = ac.config != ''

      if (hasAnEdge && sqlNode && ourNode) {
        const existingEdge = getConnectedEdges([sqlNode, ourNode], []).find(
          edge => edge.targetHandle === connectorHandle || edge.sourceHandle === connectorHandle
        )

        if (!existingEdge) {
          const sourceId = ac.direction === Direction.INPUT ? ac.uuid : 'sql'
          const targetId = ac.direction === Direction.INPUT ? 'sql' : ac.uuid
          const sourceHandle = ac.direction === Direction.INPUT ? null : connectorHandle
          const targetHandle = ac.direction === Direction.INPUT ? connectorHandle : null

          addEdges({
            id: randomString(),
            source: sourceId,
            target: targetId,
            sourceHandle: sourceHandle,
            targetHandle: targetHandle
          })
        }
      }
    },
    [getNode, getNodes, setNodes, addNodes, addEdges]
  )

  return addNewConnector
}

// When we click on input or output placeholder, we (a) add a new node at the
// position where the placeholder was, and (b) re-add the placeholder node a bit
// below its former position.
export function useAddIoNode(id: NodeProps['id']) {
  const { getNode, setNodes, addNodes } = useReactFlow()
  const onClick = (connector: ConnectorDescr, ac: AttachedConnector) => {
    // The parent is the placeholder we just clicked
    const parentNode = getNode(id)
    if (!parentNode) {
      return
    }

    // Input or Output?
    const newNodeType = parentNode.id === 'inputPlaceholder' ? 'inputNode' : 'outputNode'

    setNodes(nodes =>
      nodes.map(node => {
        // Move the placeholder node down a bit
        if (node.id === id) {
          return {
            ...node,
            position: { x: parentNode.position.x, y: parentNode.position.y + HEIGHT_OFFSET }
          }
        }

        return node
      })
    )

    addNodes({
      position: { x: parentNode.position.x, y: parentNode.position.y },
      id: ac.uuid,
      type: newNodeType,
      deletable: true,
      data: { connector, ac: ac }
    })
  }

  return onClick
}

export default useAddIoNode