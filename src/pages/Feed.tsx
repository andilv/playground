import { useNostr } from "../hooks/useNostr";
import { Kind } from "nostr-tools";

export function Feed() {
  const { events } = useNostr({
    filter: {
      kinds: [Kind.Text],
      limit: 10,
    },
  });

  return (
    <div className="flex flex-col items-center justify-center min-h-screen py-2">
      <h1 className="text-4xl font-bold">Nostr Feed</h1>
      <div className="mt-4">
        {events.map((event) => (
          <p key={event.id}>{event.content}</p> // Placeholder for Note component
        ))}
      </div>
    </div>
