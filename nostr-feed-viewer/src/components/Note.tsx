import { Event } from "nostr-tools";
import { useAuthor } from "../hooks/useAuthor";

interface NoteProps {
  event: Event;
}

export function Note({ event }: NoteProps) {
  const { author } = useAuthor({ pubkey: event.pubkey });

  const date = new Date(event.created_at * 1000);
  const formattedDate = date.toLocaleString();

  return (
    <div className="border p-4 my-2 rounded-md shadow-sm">
      <div className="flex items-center mb-2">
        <p className="font-bold mr-2">{author?.profile?.name || event.pubkey}</p>
        <p className="text-gray-500 text-sm">{formattedDate}</p>
      </div>
      <p>{event.content}</p>
    </div>
  );
}

