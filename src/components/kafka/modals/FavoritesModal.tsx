import { Button } from '../../ui/button';
import { Star, Trash2 } from 'lucide-react';
import { Dialog, DialogHeader, DialogTitle, DialogDescription } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { FavoriteMessage, KafkaMessage } from '../types';

interface FavoritesModalProps {
  favorites: FavoriteMessage[];
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onRemoveFavorite: (message: FavoriteMessage) => void;
  onSelectMessage: (message: KafkaMessage) => void;
}

export function FavoritesModal({ favorites, open, onOpenChange, onRemoveFavorite, onSelectMessage }: FavoritesModalProps) {
  const formatJson = (jsonString: string) => {
    try {
      return JSON.stringify(JSON.parse(jsonString), null, 2);
    } catch {
      return jsonString;
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContentNoClose className="max-w-5xl max-h-[90vh] bg-[#0f172b] border-[#314158] text-slate-50">
        <DialogHeader className="border-b border-[#314158] pb-4">
          <DialogTitle className="font-['Fira_Code:Retina',_sans-serif] text-[#90a1b9] text-lg">
            Favorite Messages ({favorites.length})
          </DialogTitle>
          <DialogDescription className="font-['Fira_Code:Retina',_sans-serif] text-[#62748E] text-sm">
            Add messages to your favorites by clicking the star icon in message details
          </DialogDescription>
        </DialogHeader>
        
        <div className="flex-1 overflow-auto">
          {favorites.length === 0 ? (
            <div className="flex items-center justify-center py-16">
              <div className="text-center">
                <Star className="size-12 text-[#62748E] mx-auto mb-4" />
                <div className="font-['Fira_Code:Retina',_sans-serif] text-[#90a1b9] text-lg mb-2">
                  No favorite messages yet
                </div>
                <div className="font-['Fira_Code:Retina',_sans-serif] text-[#62748E] text-sm">
                  Add messages to your favorites by clicking the star icon in message details
                </div>
              </div>
            </div>
          ) : (
            <div className="space-y-4 pt-4">
              {favorites.map((favorite, index) => (
                <div 
                  key={`${favorite.topicName}-${favorite.partition}-${favorite.offset}-${index}`} 
                  className="border border-[#314158] rounded-lg p-4 hover:bg-[#1e293b] transition-colors cursor-pointer"
                  onClick={() => onSelectMessage(favorite)}
                >
                  {/* First row: Topic name (left) + Delete button (right) */}
                  <div className="flex items-center justify-between mb-2">
                    <div className="font-['Fira_Code:Retina',_sans-serif] text-[#ffb86a]">
                      {favorite.topicName}
                    </div>
                    <Button
                      onClick={(e: any) => {
                        e.stopPropagation();
                        onRemoveFavorite(favorite);
                      }}
                      variant="outline"
                      size="sm"
                      className="bg-transparent border-[#314158] text-[#ef4444] hover:bg-[#314158] hover:text-[#f87171]"
                    >
                      <Trash2 className="size-4" />
                    </Button>
                  </div>
                  
                  {/* Second row: partition and offset */}
                  <div className="flex items-center gap-4 mb-2">
                    <div className="font-['Fira_Code:Retina',_sans-serif] text-[#90a1b9] text-sm">
                      partition: {favorite.partition}
                    </div>
                    <div className="font-['Fira_Code:Retina',_sans-serif] text-[#90a1b9] text-sm">
                      offset: {favorite.offset}
                    </div>
                  </div>
                  
                  {/* Third row: Saved timestamp */}
                  <div className="font-['Fira_Code:Retina',_sans-serif] text-[#62748E] text-sm mb-3">
                    Saved: {favorite.savedAt}
                  </div>
                  
                  {/* Fourth row: Message content */}
                  <div className="bg-[#020618] border border-[#314158] rounded-lg p-3 max-h-32 overflow-auto">
                    <div className="font-['Fira_Code:Retina',_sans-serif] text-sm text-[#90a1b9] truncate">
                      {favorite.message}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </DialogContentNoClose>
    </Dialog>
  );
}