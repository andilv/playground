import { useNostr } from "@nostrify/react";
import { Kind } from "nostr-tools";
import { Note } from "./Note"; // Will be created in the next step

export function Feed() {
  const { events, isLoading } = useNostr({
    filter: {
      kinds: [Kind.Text],
      limit: 10,
    },
  });

  if (isLoading) {
    return <div className="text-center py-8">Loading feed...</div>;
  }

  if (!events || events.length === 0) {
    return <div className="text-center py-8">No notes found.</div>;
  }

  return (
    <div className="space-y-4">
      {events.map((event) => (
        <Note key={event.id} event={event} />
      ))}
    </div>
  );
}


