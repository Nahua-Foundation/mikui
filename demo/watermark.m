// Накладывает на скриншот дисклеймер: данные придуманы, кластер не настоящий.
//
// Собирается и запускается через `watermark.sh` — там же и вся справка по
// ключам. Отдельным файлом на ObjC, а не скриптом на Python, по скучной
// причине: ни ImageMagick, ни Pillow на машине нет, а CoreText с ImageIO есть
// всегда, потому что это macOS.
//
//   clang -fobjc-arc -framework Foundation -framework CoreGraphics \
//         -framework CoreText -framework ImageIO -framework CoreServices \
//         -o watermark watermark.m
//
// Что рисуется:
//   * полоса под картинкой с текстом дисклеймера — она ничего не закрывает;
//   * повторяющаяся надпись по диагонали поверх кадра, полупрозрачная: она
//     переживает обрезку, но читать интерфейс не мешает.

#import <CoreGraphics/CoreGraphics.h>
#import <CoreText/CoreText.h>
#import <Foundation/Foundation.h>
#import <ImageIO/ImageIO.h>

/// Палитра штампа. Две штуки, потому что у приложения две темы, а полоса,
/// подобранная под тёмную, под светлым скриншотом читается чёрной врезкой —
/// будто кадр обрезали, а не подписали.
typedef struct {
  CGFloat background[4];
  CGFloat rule[4];
  CGFloat text[4];
  CGFloat accent[4];
  /// Цвет диагональной плитки. Отдельно от текста полосы: на белом фоне
  /// светло-серая надпись при любой разумной прозрачности не видна вовсе.
  CGFloat tile[4];
  /// Множитель прозрачности плитки: тёмная надпись на светлом фоне при той же
  /// альфе теряется сильнее, чем светлая на тёмном.
  CGFloat tileBoost;
} Palette;

static const Palette kDark = {
    .background = {0.043, 0.047, 0.063, 1.0},
    .rule = {0.16, 0.17, 0.20, 1.0},
    .text = {0.62, 0.64, 0.70, 1.0},
    .accent = {0.98, 0.75, 0.36, 1.0},
    .tile = {0.75, 0.78, 0.85, 1.0},
    .tileBoost = 1.0,
};

static const Palette kLight = {
    .background = {0.949, 0.953, 0.961, 1.0},
    .rule = {0.82, 0.83, 0.85, 1.0},
    .text = {0.27, 0.29, 0.33, 1.0},
    .accent = {0.62, 0.36, 0.04, 1.0},
    .tile = {0.10, 0.12, 0.16, 1.0},
    .tileBoost = 1.15,
};

static CGColorRef color(const CGFloat components[4], CGFloat alpha) {
  CGColorSpaceRef space = CGColorSpaceCreateWithName(kCGColorSpaceSRGB);
  CGFloat scaled[4] = {components[0], components[1], components[2], components[3] * alpha};
  CGColorRef result = CGColorCreate(space, scaled);
  CGColorSpaceRelease(space);
  return result;
}

static CTFontRef font(CGFloat size, bool bold) {
  // Menlo — тот же моноширинный дух, что у интерфейса; если его вдруг нет,
  // CoreText сам подставит системный, и это лучше, чем падение.
  CTFontRef chosen = CTFontCreateWithName(bold ? CFSTR("Menlo-Bold") : CFSTR("Menlo"), size, NULL);
  return chosen;
}

static CTLineRef line(NSString *text, CGFloat size, bool bold, CGColorRef fill, CGFloat tracking) {
  CTFontRef used = font(size, bold);
  NSDictionary *attributes = @{
    (id)kCTFontAttributeName : (__bridge id)used,
    (id)kCTForegroundColorAttributeName : (__bridge id)fill,
    (id)kCTKernAttributeName : @(tracking),
  };
  NSAttributedString *string = [[NSAttributedString alloc] initWithString:text attributes:attributes];
  CTLineRef result = CTLineCreateWithAttributedString((CFAttributedStringRef)string);
  CFRelease(used);
  return result;
}

static CGFloat lineWidth(CTLineRef target) {
  return (CGFloat)CTLineGetTypographicBounds(target, NULL, NULL, NULL);
}

/// Диагональная плитка из повторяющейся надписи.
///
/// Рисуется в повёрнутой системе координат по сетке с шагом, кратным ширине
/// надписи: так соседние ряды не складываются в вертикальные полосы, а
/// смещение через ряд не даёт узору выглядеть машинным.
static void drawTiles(CGContextRef ctx, const Palette *palette, NSString *text, CGFloat width,
                      CGFloat height, CGFloat size, CGFloat opacity, CGFloat angle) {
  CGColorRef fill = color(palette->tile, opacity * palette->tileBoost);
  CTLineRef tile = line(text, size, true, fill, 2.0);
  CGFloat stride = lineWidth(tile) + size * 6.0;
  CGFloat step = size * 7.0;
  CGFloat reach = hypot(width, height);

  CGContextSaveGState(ctx);
  CGContextTranslateCTM(ctx, width / 2.0, height / 2.0);
  CGContextRotateCTM(ctx, angle * (CGFloat)M_PI / 180.0);

  int row = 0;
  for (CGFloat y = -reach; y <= reach; y += step, row++) {
    CGFloat shift = (row % 2 == 0) ? 0.0 : stride / 2.0;
    for (CGFloat x = -reach + shift; x <= reach; x += stride) {
      CGContextSetTextPosition(ctx, x, y);
      CTLineDraw(tile, ctx);
    }
  }

  CGContextRestoreGState(ctx);
  CFRelease(tile);
  CGColorRelease(fill);
}

/// Полоса под кадром: метка и дисклеймер.
static void drawStrip(CGContextRef ctx, const Palette *palette, CGFloat width, CGFloat stripHeight,
                      CGFloat scale, NSString *badge, NSString *disclaimer) {
  CGColorRef background = color(palette->background, 1.0);
  CGContextSetFillColorWithColor(ctx, background);
  CGContextFillRect(ctx, CGRectMake(0, 0, width, stripHeight));
  CGColorRelease(background);

  // Тонкая линия на стыке: без неё полоса сливается с тёмным кадром и выглядит
  // обрезком скриншота, а не подписью к нему.
  CGColorRef rule = color(palette->rule, 1.0);
  CGContextSetFillColorWithColor(ctx, rule);
  CGContextFillRect(ctx, CGRectMake(0, stripHeight - scale, width, scale));
  CGColorRelease(rule);

  CGFloat size = 15.0 * scale;
  CGFloat padding = 26.0 * scale;
  CGFloat baseline = (stripHeight - size) / 2.0 + size * 0.22;

  CGColorRef accent = color(palette->accent, 1.0);
  CTLineRef marker = line(badge, size, true, accent, 2.5);
  CGContextSetTextPosition(ctx, padding, baseline);
  CTLineDraw(marker, ctx);
  CGFloat used = padding + lineWidth(marker) + size * 1.4;
  CFRelease(marker);
  CGColorRelease(accent);

  CGColorRef text = color(palette->text, 1.0);
  CTLineRef body = line(disclaimer, size, false, text, 0.5);
  CGContextSetTextPosition(ctx, used, baseline);
  CTLineDraw(body, ctx);
  CFRelease(body);
  CGColorRelease(text);
}

/// Светлый скриншот или тёмный.
///
/// Считается не по одному пикселю в углу, а по всему кадру, сжатому в один
/// пиксель: это и есть средний цвет, и рисовать его умеет сам CoreGraphics.
/// Угол обманул бы на первом же скриншоте со светлой модалкой поверх тёмного
/// фона — или наоборот.
static bool isLight(CGImageRef image) {
  unsigned char pixel[4] = {0, 0, 0, 0};
  CGColorSpaceRef space = CGColorSpaceCreateWithName(kCGColorSpaceSRGB);
  CGContextRef tiny = CGBitmapContextCreate(pixel, 1, 1, 8, 4, space,
                                            (CGBitmapInfo)kCGImageAlphaPremultipliedLast);
  CGColorSpaceRelease(space);
  if (!tiny) {
    return false;
  }
  CGContextSetInterpolationQuality(tiny, kCGInterpolationMedium);
  CGContextDrawImage(tiny, CGRectMake(0, 0, 1, 1), image);
  CGContextRelease(tiny);

  CGFloat luminance =
      (0.2126 * pixel[0] + 0.7152 * pixel[1] + 0.0722 * pixel[2]) / 255.0;
  return luminance > 0.5;
}

int main(int argc, const char *argv[]) {
  @autoreleasepool {
    if (argc < 3) {
      fprintf(stderr, "usage: watermark <in.png> <out.png> [tile text] [disclaimer] [badge] [opacity]\n");
      return 2;
    }
    NSString *input = [NSString stringWithUTF8String:argv[1]];
    NSString *output = [NSString stringWithUTF8String:argv[2]];
    NSString *tile = argc > 3 ? [NSString stringWithUTF8String:argv[3]] : @"SYNTHETIC DEMO DATA";
    NSString *disclaimer = argc > 4 ? [NSString stringWithUTF8String:argv[4]]
                                    : @"Synthetic demo data, generated by AI for these screenshots.";
    NSString *badge = argc > 5 ? [NSString stringWithUTF8String:argv[5]] : @"NOT REAL DATA";
    CGFloat opacity = argc > 6 ? (CGFloat)atof(argv[6]) : 0.07;

    CGImageSourceRef source =
        CGImageSourceCreateWithURL((CFURLRef)[NSURL fileURLWithPath:input], NULL);
    if (!source) {
      fprintf(stderr, "не читается: %s\n", argv[1]);
      return 1;
    }
    CGImageRef image = CGImageSourceCreateImageAtIndex(source, 0, NULL);
    CFRelease(source);
    if (!image) {
      fprintf(stderr, "не разбирается как изображение: %s\n", argv[1]);
      return 1;
    }

    const Palette *palette = isLight(image) ? &kLight : &kDark;
    CGFloat width = CGImageGetWidth(image);
    CGFloat height = CGImageGetHeight(image);
    // Скриншоты с retina-экрана приходят удвоенными; кегль считаем от ширины,
    // чтобы подпись выглядела одинаково и на 1x, и на 2x.
    CGFloat scale = fmax(width / 1500.0, 1.0);
    CGFloat stripHeight = round(46.0 * scale);

    CGColorSpaceRef space = CGColorSpaceCreateWithName(kCGColorSpaceSRGB);
    CGContextRef ctx = CGBitmapContextCreate(NULL, (size_t)width, (size_t)(height + stripHeight), 8, 0,
                                             space, (CGBitmapInfo)kCGImageAlphaPremultipliedLast);
    CGColorSpaceRelease(space);
    if (!ctx) {
      fprintf(stderr, "не создался контекст %gx%g\n", width, height + stripHeight);
      CGImageRelease(image);
      return 1;
    }

    CGContextDrawImage(ctx, CGRectMake(0, stripHeight, width, height), image);
    CGImageRelease(image);

    // Плитка — только по кадру, полосу она бы засорила.
    CGContextSaveGState(ctx);
    CGContextClipToRect(ctx, CGRectMake(0, stripHeight, width, height));
    CGContextTranslateCTM(ctx, 0, stripHeight);
    drawTiles(ctx, palette, tile, width, height, 22.0 * scale, opacity, 30.0);
    CGContextRestoreGState(ctx);

    drawStrip(ctx, palette, width, stripHeight, scale, badge, disclaimer);

    CGImageRef result = CGBitmapContextCreateImage(ctx);
    CGContextRelease(ctx);

    // Идентификатор типа строкой, а не `kUTTypePNG`: тот объявлен устаревшим, а
    // замена ему живёт в UniformTypeIdentifiers, который из ObjC тянуть дороже,
    // чем написать здесь ту же константу.
    CGImageDestinationRef destination = CGImageDestinationCreateWithURL(
        (CFURLRef)[NSURL fileURLWithPath:output], CFSTR("public.png"), 1, NULL);
    if (!destination) {
      fprintf(stderr, "не пишется: %s\n", argv[2]);
      CGImageRelease(result);
      return 1;
    }
    CGImageDestinationAddImage(destination, result, NULL);
    bool written = CGImageDestinationFinalize(destination);
    CFRelease(destination);
    CGImageRelease(result);
    if (!written) {
      fprintf(stderr, "не записалось: %s\n", argv[2]);
      return 1;
    }
    return 0;
  }
}
