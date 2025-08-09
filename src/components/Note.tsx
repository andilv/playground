import React from 'react';
import { Event } from 'nostr-tools';

interface NoteProps {
  event: Event;
}

const Note: React.FC<NoteProps> = ({ event }) => {
  return (
    <div className="border p-4 mb-2 rounded-md">
      <p className="text-gray-500 text-sm">Public Key: {event.pubkey}</p>
      <p className="text-lg">{event.content}</p>
      <p className="text-gray-400 text-xs">
        {new Date(event.created_at * 1000).toLocaleString()}
      </p>
    </div>
  );
};

export default Note;
