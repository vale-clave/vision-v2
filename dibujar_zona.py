import cv2

IMAGE_PATH = "images/trinidad-terraza.png"  # cambia a la ruta real

points = []

def click_event(event, x, y, flags, param):
    if event == cv2.EVENT_LBUTTONDOWN:
        points.append([x, y])
        print(f"punto: [{x}, {y}]")
        img = param.copy()
        # dibuja puntos y líneas
        for i, (px, py) in enumerate(points):
            cv2.circle(img, (px, py), 4, (0, 0, 255), -1)
            cv2.putText(img, str(i+1), (px+5, py-5),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 1)
        if len(points) > 1:
            for i in range(len(points)-1):
                cv2.line(img, tuple(points[i]), tuple(points[i+1]), (255, 0, 0), 2)
        cv2.imshow("Definir zona", img)

img = cv2.imread(IMAGE_PATH)
if img is None:
    raise RuntimeError("No pude cargar la imagen")

cv2.namedWindow("Definir zona")
cv2.setMouseCallback("Definir zona", click_event, img)

cv2.imshow("Definir zona", img)
print("Haz click en los vértices de la zona; cierra la ventana o presiona ESC cuando termines.")
cv2.waitKey(0)
cv2.destroyAllWindows()

print("\nPolygon para config.yaml:")
print(points)