
import logging
import os
import sys
from pathlib import Path
import torch
import torch.backends.cudnn as cudnn
import numpy as np

FILE = Path(__file__).resolve()

ROOT = FILE.parents[0] 

if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT)) 
ROOT = Path(os.path.relpath(ROOT, Path.cwd()))


from models.common import DetectMultiBackend
from utils.general import ( Profile, check_img_size, cv2,
                           non_max_suppression, scale_coords, xyxy2xywh, 
                           clip_coords, xywh2xyxy)
from utils.plots import Annotator, colors
from utils.segment.general import process_mask, scale_masks
from utils.segment.plots import plot_masks, bw_mask
from utils.torch_utils import select_device
from utils.augmentations import letterbox



def init():
    """
    This function is called when the container is initialized/started, typically after create/update of the deployment.
    You can write the logic here to perform init operations like caching the model in memory
    """
    global model
    global imgsz, conf_thres, iou_thres, max_det, stride, pt, device, names
    
        # AZUREML_MODEL_DIR is an environment variable created during deployment.
        # It is the path to the model folder (./azureml-models/$MODEL_NAME/$VERSION)
        # Please provide your model's folder name if there is one
    weights = Path("model/yolov7_seg_final.pt")




    imgsz=(640, 640)  # inference size (height, width)
    conf_thres=0.25  # confidence threshold
    iou_thres=0.55  # NMS IOU threshold
    max_det=1  # maximum detections per image
    device=''  # cuda device, i.e. 0 or 0,1,2,3 or cpu





    half=False  # use FP16 half-precision inference
    dnn=False  # use OpenCV DNN for ONNX inference

   
   


    device = select_device(device)
    model = DetectMultiBackend(weights, device=device, dnn=dnn, data='', fp16=half)
    stride, names, pt = model.stride, model.names, model.pt
    imgsz = check_img_size(imgsz, s=stride)

    model.warmup(imgsz=(1 , 3, *imgsz))

    logging.info("Init complete")


def run(binData, classlist):

    classes = classlist
    check_ok = False
    dt = (Profile(), Profile(), Profile())

    image = np.asarray(bytearray(binData), dtype="uint8")
    image = cv2.imdecode(image, cv2.IMREAD_COLOR)
    im0s = image
    im = letterbox(im0s, imgsz, stride=stride, auto=pt)[0]  # padded resize
    im = im.transpose((2, 0, 1))[::-1]  # HWC to CHW, BGR to RGB
    im = np.ascontiguousarray(im)


 
    with dt[0]:
        im = torch.from_numpy(im).to(device)
        im = im.half() if model.fp16 else im.float()  # uint8 to fp16/32
        im /= 255  # 0 - 255 to 0.0 - 1.0
        if len(im.shape) == 3:
            im = im[None]  # expand for batch dim

    # Inference
    with dt[1]:
        pred, out = model(im, augment= False, visualize= False)
        proto = out[1]

    # NMS
    with dt[2]:
        pred = non_max_suppression(pred, conf_thres, iou_thres, classes, False, max_det=max_det, nm=32)

    # Second-stage classifier (optional)
    # pred = utils.general.apply_classifier(pred, classifier_model, im, im0s)

    # Process predictions
    for i, det in enumerate(pred):  # per image
       
        im0 = im0s.copy() 
        annotator = Annotator(im0, line_width=2, example=str(names))
        if len(det):
            masks = process_mask(proto[i], det[:, 6:], det[:, :4], im.shape[2:], upsample=True)  # HWC

            # Rescale boxes from img_size to im0 size
            det[:, :4] = scale_coords(im.shape[2:], det[:, :4], im0.shape).round()

                
                

            # Print results
            for c in det[:, 5].unique():
                n = (det[:, 5] == c).sum()  # detections per class
                
   
                

            # Mask plotting ----------------------------------------------------------------------------------------
            mcolors = [colors(int(6), True) for cls in det[:, 5]]
            im_masks = plot_masks(im[i], masks, mcolors)  # image with masks shape(imh,imw,3)
            annotator.im = scale_masks(im.shape[2:], im_masks, im0.shape)
                # scale to original h, w
            bwMask =  bw_mask(im[i],masks) 
            bwMask = scale_masks(im.shape[2:], bwMask, im0.shape)
            check_ok = True
            
            #pred_data
            *xyxy, conf, cls = reversed(det[:, :6])[0]


        # Save results (image with detections)
        if check_ok:

            segmented_img = cv2.bitwise_and(im0s,bwMask)
            tmp = cv2.cvtColor(segmented_img, cv2.COLOR_BGR2GRAY)
            _, alpha = cv2.threshold(tmp, 0, 255, cv2.THRESH_BINARY)
            b, g, r = cv2.split(segmented_img)
            rgba = [b, g, r, alpha]
            dst = cv2.merge(rgba, 4) #transpng

            xyxyt = torch.tensor(xyxy).view(-1, 4)
            b = xyxy2xywh(xyxyt)  # boxes
            b[:, 2:] = b[:, 2:] * 1.08  + 10  # box wh * gain + pad
            xyxy = xywh2xyxy(b).long()
            clip_coords(xyxy, dst.shape)
            crop = dst[int(xyxy[0, 1]):int(xyxy[0, 3]), int(xyxy[0, 0]):int(xyxy[0, 2]), ::(1)]


            img_encode = cv2.imencode('.png', crop)[1]
            data_encode = np.array(img_encode)
            byte_encode = data_encode.tobytes()


           
            
            predClass = names[int(cls)]
            confd = "%.2f" % float(conf)
            infTime = str(int(dt[1].dt * 1E3)) + ' ms'
            return predClass, confd, infTime, byte_encode
           

        else:
            print('checkok false')
            return 0
            
         

    
                
        
    










