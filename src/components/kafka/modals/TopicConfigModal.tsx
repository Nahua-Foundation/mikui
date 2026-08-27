import { useState } from 'react';
import { Button } from '../../ui/button';
import { Upload, Bookmark } from 'lucide-react';
import { toast } from 'sonner';
import { Dialog, DialogHeader, DialogTitle } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../ui/select';
import { Input } from '../../ui/input';
import { Label } from '../../ui/label';
import { Topic } from '../types';

interface TopicConfigModalProps {
  topic: Topic | null; 
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function TopicConfigModal({ topic, open, onOpenChange }: TopicConfigModalProps) {
  const [messageType, setMessageType] = useState<string>('JSON');
  const [schemaRegistry, setSchemaRegistry] = useState<string>('');
  const [password, setPassword] = useState<string>('');

  const handleLoadFiles = () => {
    const input = document.createElement('input');
    input.type = 'file';
    input.multiple = true;
    input.accept = '.proto';
    input.onchange = (e) => {
      const files = (e.target as HTMLInputElement).files;
      if (files && files.length > 0) {
        toast.success(`Loaded ${files.length} Proto file(s)`);
      }
    };
    input.click();
  };

  const handleSave = () => {
    toast.success(`Configuration saved for topic: ${topic?.name}`);
    onOpenChange(false);
  };

  const handleAddToFavorites = () => {
    toast.success(`Added "${topic?.name}" to favorites`);
  };

  if (!topic) return null;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContentNoClose 
        className="max-w-md bg-surface border-edge text-slate-50"
        aria-describedby={undefined}
      >
        <DialogHeader className="border-b border-edge pb-4">
          <div className="flex items-center justify-between">
            <DialogTitle className="font-mono text-soft text-lg">
              {topic.name}
            </DialogTitle>
            <button
              onClick={handleAddToFavorites}
              className="p-1 text-dim hover:text-brand transition-colors duration-200 cursor-pointer border-none bg-transparent outline-none"
              title="Add to favorites"
            >
              <Bookmark className="size-4" />
            </button>
          </div>
        </DialogHeader>
        
        <div className="space-y-6 pt-4">
          {/* Message Type */}
          <div className="space-y-2">
            <Label className="font-mono text-sm text-soft">
              Message Type
            </Label>
            <Select value={messageType} onValueChange={setMessageType}>
              <SelectTrigger className="bg-surface border-edge text-slate-50 font-mono">
                <SelectValue />
              </SelectTrigger>
              <SelectContent className="bg-surface border-edge">
                <SelectItem value="JSON" className="text-slate-50 font-mono focus:bg-edge">JSON</SelectItem>
                <SelectItem value="Text" className="text-slate-50 font-mono focus:bg-edge">Text</SelectItem>
                <SelectItem value="Proto" className="text-slate-50 font-mono focus:bg-edge">Proto</SelectItem>
                <SelectItem value="Avro" className="text-slate-50 font-mono focus:bg-edge">Avro</SelectItem>
              </SelectContent>
            </Select>
          </div>

          {/* Proto specific fields */}
          {messageType === 'Proto' && (
            <div className="space-y-4">
              <div className="space-y-2">
                <Label className="font-mono text-sm text-soft">
                  Proto Files
                </Label>
                <Button
                  onClick={handleLoadFiles}
                  variant="outline"
                  className="w-full bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono"
                >
                  <Upload className="size-4 mr-2" />
                  Load Proto Files
                </Button>
              </div>
            </div>
          )}

          {/* Avro specific fields */}
          {messageType === 'Avro' && (
            <div className="space-y-4">
              <div className="space-y-2">
                <Label className="font-mono text-sm text-soft">
                  Schema Registry URL
                </Label>
                <Input
                  value={schemaRegistry}
                  onChange={(e) => setSchemaRegistry(e.target.value)}
                  placeholder="http://localhost:8081"
                  className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim"
                />
              </div>
              <div className="space-y-2">
                <Label className="font-mono text-sm text-soft">
                  Password
                </Label>
                <Input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="Enter password"
                  className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim"
                />
              </div>
            </div>
          )}

          {/* Action Buttons */}
          <div className="flex gap-3 pt-4">
            <Button
              onClick={() => onOpenChange(false)}
              variant="outline"
              className="flex-1 bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono"
            >
              Cancel
            </Button>
            <Button
              onClick={handleSave}
              className="flex-1 bg-brand text-surface hover:bg-brand-hover font-mono"
            >
              Save
            </Button>
          </div>
        </div>
      </DialogContentNoClose>
    </Dialog>
  );
}