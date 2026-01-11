import React from 'react';
import { NostrProvider } from 'react-nostr';
import Feed from './components/Feed';

const relayUrls = [
  'wss://relay.damus.io',
  'wss://relay.snort.social',
  'wss://nostr.wine',
];

function App() {
  return (
    <NostrProvider relayUrls={relayUrls}>
      <div className="App">
        <Feed />
      </div>
    </NostrProvider>
  );
}

