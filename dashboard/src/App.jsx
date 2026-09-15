import { useState, useEffect } from 'react'
import axios from 'axios'
import { formatDistanceToNow } from 'date-fns'
import './App.css'

const API_BASE_URL = 'http://localhost:8080'

function App() {
  const [endpointId, setEndpointId] = useState('test-endpoint')
  const [events, setEvents] = useState([])
  const [selectedEvent, setSelectedEvent] = useState(null)
  const [loading, setLoading] = useState(false)
  const [replaying, setReplaying] = useState(false)

  const fetchEvents = async () => {
    if (!endpointId) return
    setLoading(true)
    try {
      const response = await axios.get(`${API_BASE_URL}/events/${endpointId}`)
      setEvents(response.data || [])
    } catch (err) {
      console.error('Failed to fetch events', err)
      setEvents([])
    } finally {
      setLoading(false)
    }
  }

  // Polling for updates
  useEffect(() => {
    fetchEvents()
    const interval = setInterval(fetchEvents, 3000)
    return () => clearInterval(interval)
  }, [endpointId])

  const handleReplay = async (eventToReplay) => {
    setReplaying(true)
    try {
      await axios.post(`${API_BASE_URL}/webhook/${endpointId}`, eventToReplay.payload, {
        headers: {
          'Content-Type': 'application/json',
          // Re-add some essential original headers if needed, but typically standard headers suffice for replay
        }
      })
      // Instantly fetch updates
      fetchEvents()
    } catch (err) {
      console.error('Failed to replay event', err)
      alert('Failed to replay event. Ensure the endpoint exists.')
    } finally {
      setReplaying(false)
    }
  }

  const getStatusBadgeClass = (status) => {
    switch (status) {
      case 'delivered': return 'badge-success'
      case 'pending': return 'badge-warning'
      case 'dead_letter': return 'badge-danger'
      default: return 'badge-secondary'
    }
  }

  return (
    <div className="dashboard-container">
      <header className="dashboard-header">
        <div className="logo-section">
          <div className="logo-icon"></div>
          <h1>Hook Observer</h1>
        </div>
        <div className="endpoint-selector">
          <label>Monitoring Endpoint:</label>
          <input 
            type="text" 
            value={endpointId} 
            onChange={(e) => setEndpointId(e.target.value)}
            placeholder="e.g. test-endpoint"
          />
        </div>
      </header>

      <main className="dashboard-main">
        <section className="events-panel">
          <div className="panel-header">
            <h2>Recent Events {loading && <span className="spinner"></span>}</h2>
            <div className="event-count">{events.length} total</div>
          </div>
          <div className="events-list">
            {events.length === 0 && !loading ? (
              <div className="empty-state">No events found for this endpoint.</div>
            ) : (
              events.map((evt) => (
                <div 
                  key={evt.id} 
                  className={`event-card ${selectedEvent?.id === evt.id ? 'selected' : ''}`}
                  onClick={() => setSelectedEvent(evt)}
                >
                  <div className="event-card-header">
                    <span className="event-id">{evt.id.substring(0, 8)}...</span>
                    <span className={`badge ${getStatusBadgeClass(evt.status)}`}>{evt.status}</span>
                  </div>
                  <div className="event-card-time">
                    {formatDistanceToNow(new Date(evt.received_at), { addSuffix: true })}
                  </div>
                </div>
              ))
            )}
          </div>
        </section>

        <section className="details-panel">
          {selectedEvent ? (
            <div className="event-details">
              <div className="details-header">
                <h2>Event Details</h2>
                <button 
                  className="replay-btn" 
                  onClick={() => handleReplay(selectedEvent)}
                  disabled={replaying}
                >
                  {replaying ? 'Replaying...' : 'Replay Event'}
                </button>
              </div>
              
              <div className="details-grid">
                <div className="detail-item">
                  <label>Event ID</label>
                  <div>{selectedEvent.id}</div>
                </div>
                <div className="detail-item">
                  <label>Status</label>
                  <div><span className={`badge ${getStatusBadgeClass(selectedEvent.status)}`}>{selectedEvent.status}</span></div>
                </div>
                <div className="detail-item">
                  <label>Received At</label>
                  <div>{new Date(selectedEvent.received_at).toLocaleString()}</div>
                </div>
              </div>

              <div className="code-block-wrapper">
                <label>Headers</label>
                <pre className="code-block">
                  <code>{JSON.stringify(selectedEvent.headers, null, 2)}</code>
                </pre>
              </div>

              <div className="code-block-wrapper">
                <label>Payload</label>
                <pre className="code-block">
                  <code>{JSON.stringify(selectedEvent.payload, null, 2)}</code>
                </pre>
              </div>
            </div>
          ) : (
            <div className="empty-state details-empty">
              <div className="empty-icon">Select an event</div>
              <p>Click on any event in the list to view its payload and headers, or to replay it.</p>
            </div>
          )}
        </section>
      </main>
    </div>
  )
}

export default App
