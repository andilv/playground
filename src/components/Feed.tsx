import React, { useRef } from 'react';
import { useNostr } from 'react-nostr';
import { useVirtual } from '@tanstack/react-virtual';
import Note from './Note';

const Feed: React.FC = () => {
  const { events } = useNostr([
    { kinds: [1], limit: 100 }, // Fetch text notes
  ]);

  const parentRef = useRef<HTMLDivElement>(null);

  const rowVirtualizer = useVirtual({
    parentRef,
    size: events.length,
    estimateSize: React.useCallback(() => 100, []), // Estimate row height
    overscan: 5,
  });

  return (
    <div
      ref={parentRef}
      className="List"
      style={{
        height: `500px`,
        overflow: 'auto',
      }}
    >
      <div
        style={{
          height: `${rowVirtualizer.totalSize}px`,
          width: '100%',
          position: 'relative',
        }}
      >
        {rowVirtualizer.virtualItems.map((virtualItem) => {
          const event = events[virtualItem.index];
          return (
            <div
              key={virtualItem.index}
              ref={rowVirtualizer.measureElement}
              data-index={virtualItem.index}
              style={{
                position: 'absolute',
                top: 0,
                left: 0,
                width: '100%',
                transform: `translateY(${virtualItem.start}px)`,
              }}
            >
